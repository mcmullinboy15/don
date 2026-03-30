use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub services: HashMap<String, Service>,
    /// One-shot tasks (migrations, codegen, etc).
    /// Only re-run when watched files change since last successful run.
    #[serde(default)]
    pub tasks: HashMap<String, Task>,
    /// Named profiles — subsets of services/tasks to run.
    #[serde(default)]
    pub profiles: HashMap<String, Profile>,
}

/// A named subset of services and tasks to run.
/// Transitive dependencies are automatically included.
#[derive(Debug, Clone, Deserialize)]
pub struct Profile {
    #[serde(default)]
    pub services: Vec<String>,
    #[serde(default)]
    pub tasks: Vec<String>,
}

/// A one-shot task that runs to completion.
/// Tasks can depend on services (waits for ready) and other tasks.
/// File watching determines whether the task needs to re-run.
#[derive(Debug, Clone, Deserialize)]
pub struct Task {
    /// The command to execute
    pub cmd: String,
    /// Arguments to pass to the command
    #[serde(default)]
    pub args: Vec<String>,
    /// Working directory
    pub dir: Option<PathBuf>,
    /// Environment variables
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Services or tasks that must be ready/complete before this task runs
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// File glob patterns — task only re-runs if these files changed since last success.
    /// If empty, the task always runs.
    #[serde(default)]
    pub watch: Vec<String>,
    /// Maximum time the task is allowed to run (e.g. "5m", "30s"). No timeout by default.
    pub timeout: Option<String>,
    /// Where to send stdout/stderr. Defaults to stdout.
    #[serde(default)]
    pub log: LogConfig,
}

#[derive(Debug, Deserialize)]
pub struct Service {
    /// Working directory for the service
    pub dir: Option<PathBuf>,
    /// Environment variables
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Paths to env files to load. Don also auto-loads `.env.<service-name>` if it exists.
    #[serde(default)]
    pub env_file: Vec<PathBuf>,
    /// File glob patterns to watch for rebuilding/restarting
    #[serde(default)]
    pub watch: Vec<String>,
    /// Debounce window for file watch events (e.g. "500ms"). Defaults to "200ms".
    pub debounce: Option<String>,
    /// Services that must be started before this one
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Addresses for don to listen on and pass to the service via LISTEN_FDS.
    /// Don holds the sockets open across restarts so traffic is never dropped.
    #[serde(default)]
    pub listen: Vec<String>,
    /// Optional binary download configuration for this service
    pub download: Option<DownloadConfig>,
    /// Ready check — used to gate dependents until this service is accepting traffic
    pub ready: Option<ReadyCheck>,
    /// Shutdown behavior
    pub shutdown: Option<ShutdownConfig>,
    /// Where to send stdout/stderr. Defaults to stdout.
    #[serde(default)]
    pub log: LogConfig,
    /// Per-platform overrides. If the current platform has an entry here,
    /// its fields are merged on top of the base service config.
    #[serde(default)]
    pub platform: HashMap<Platform, ServiceOverride>,

    // -- Preset: docker --
    pub docker: Option<DockerConfig>,

    // -- Preset: rust --
    pub rust: Option<RustConfig>,

    // -- Custom service (no preset) --
    /// Command to run the service
    pub run: Option<Command>,
    /// Command to build the service before running
    pub build: Option<Command>,
}

/// A command to execute: a binary/program name plus arguments.
#[derive(Debug, Clone, Deserialize)]
pub struct Command {
    /// The binary or program to execute
    pub cmd: String,
    /// Arguments to pass to the binary
    #[serde(default)]
    pub args: Vec<String>,
}

/// Ready check configuration. Exactly one of `exec`, `tcp`, or `http` must be set.
#[derive(Debug, Clone, Deserialize)]
pub struct ReadyCheck {
    /// Run a command — exit code 0 means ready
    pub exec: Option<Command>,
    /// Connect to a TCP address — successful connection means ready
    pub tcp: Option<String>,
    /// Hit an HTTP endpoint — 2xx response means ready
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

/// Where to send a service's stdout/stderr.
///
/// In TOML, this is either a string shorthand or a table:
/// - `log = "stdout"` (default)
/// - `log = "ignore"`
/// - `log = "path/to/file.log"`
/// - `log = { file = "path/to/file.log" }` (equivalent to the string form)
#[derive(Debug, Clone, Default)]
pub enum LogConfig {
    /// Print to don's stdout, prefixed with the service name
    #[default]
    Stdout,
    /// Discard all output
    Ignore,
    /// Write to a file
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

/// Platform-specific overrides for a service. Any field set here replaces the
/// corresponding base field. For `env`, entries are merged (override wins on conflict).
/// If any preset field (docker/rust/run) is set, it completely replaces the base preset.
#[derive(Debug, Deserialize)]
pub struct ServiceOverride {
    pub dir: Option<PathBuf>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    pub env_file: Option<Vec<PathBuf>>,
    pub watch: Option<Vec<String>>,
    pub debounce: Option<String>,
    pub depends_on: Option<Vec<String>>,
    pub listen: Option<Vec<String>>,
    pub download: Option<DownloadConfig>,
    pub ready: Option<ReadyCheck>,
    pub shutdown: Option<ShutdownConfig>,
    pub log: Option<LogConfig>,

    pub docker: Option<DockerConfig>,
    pub rust: Option<RustConfig>,
    pub run: Option<Command>,
    pub build: Option<Command>,
}

/// A fully resolved service with platform overrides applied.
#[derive(Debug)]
pub struct ResolvedService {
    pub dir: Option<PathBuf>,
    pub env: HashMap<String, String>,
    pub env_file: Vec<PathBuf>,
    pub watch: Vec<String>,
    pub debounce: Option<String>,
    pub depends_on: Vec<String>,
    pub listen: Vec<String>,
    pub download: Option<DownloadConfig>,
    pub ready: Option<ReadyCheck>,
    pub shutdown: Option<ShutdownConfig>,
    pub log: LogConfig,

    pub docker: Option<DockerConfig>,
    pub rust: Option<RustConfig>,
    pub run: Option<Command>,
    pub build: Option<Command>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DockerConfig {
    /// Docker image to run (e.g. "postgres:16").
    /// For built images, this is also used as the tag for `docker build -t`.
    pub image: String,
    /// Container name — used to check if it's already running
    pub container: Option<String>,
    /// Port mappings (e.g. ["5432:5432"])
    #[serde(default)]
    pub ports: Vec<String>,
    /// Volume mounts (e.g. ["pgdata:/var/lib/postgresql/data"])
    #[serde(default)]
    pub volumes: Vec<String>,
    /// Docker network to attach to
    pub network: Option<String>,
    /// Override the container's default command / entrypoint args
    #[serde(default)]
    pub command: Vec<String>,
    /// Env files to pass to docker via --env-file
    #[serde(default)]
    pub env_file: Vec<PathBuf>,
    /// Build configuration — if set, don builds the image before running
    pub build: Option<DockerBuildConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DockerBuildConfig {
    /// Build context path (e.g. "." or "./services/api")
    pub context: String,
    /// Path to the Dockerfile, relative to context. Defaults to "Dockerfile".
    pub dockerfile: Option<String>,
    /// Build target for multi-stage builds (e.g. "development")
    pub target: Option<String>,
    /// Build arguments passed via --build-arg
    #[serde(default)]
    pub args: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RustConfig {
    /// Name of the binary target to build and run
    pub binary: String,
    /// Cargo features to enable
    #[serde(default)]
    pub features: Vec<String>,
    /// Build in release mode (default: false)
    #[serde(default)]
    pub release: bool,
    /// Extra arguments to pass to `cargo build`
    #[serde(default)]
    pub extra_args: Vec<String>,
    /// Override the cargo target directory
    pub target_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DownloadConfig {
    /// Per-platform download artifacts. Keys are "{os}-{arch}" using Rust conventions:
    /// linux-x86_64, linux-aarch64, macos-x86_64, macos-aarch64, windows-x86_64, windows-aarch64
    pub platform: HashMap<Platform, PlatformDownload>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Platform {
    LinuxX86_64,
    LinuxAarch64,
    MacosX86_64,
    MacosAarch64,
    WindowsX86_64,
    WindowsAarch64,
}

impl Platform {
    /// Returns the platform matching the current machine, or None if unsupported.
    pub fn current() -> Option<Self> {
        Self::from_os_arch(std::env::consts::OS, std::env::consts::ARCH)
    }

    fn from_os_arch(os: &str, arch: &str) -> Option<Self> {
        match (os, arch) {
            ("linux", "x86_64") => Some(Self::LinuxX86_64),
            ("linux", "aarch64") => Some(Self::LinuxAarch64),
            ("macos", "x86_64") => Some(Self::MacosX86_64),
            ("macos", "aarch64") => Some(Self::MacosAarch64),
            ("windows", "x86_64") => Some(Self::WindowsX86_64),
            ("windows", "aarch64") => Some(Self::WindowsAarch64),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::LinuxX86_64 => "linux-x86_64",
            Self::LinuxAarch64 => "linux-aarch64",
            Self::MacosX86_64 => "macos-x86_64",
            Self::MacosAarch64 => "macos-aarch64",
            Self::WindowsX86_64 => "windows-x86_64",
            Self::WindowsAarch64 => "windows-aarch64",
        }
    }

    const ALL: &[Self] = &[
        Self::LinuxX86_64,
        Self::LinuxAarch64,
        Self::MacosX86_64,
        Self::MacosAarch64,
        Self::WindowsX86_64,
        Self::WindowsAarch64,
    ];
}

impl std::fmt::Display for Platform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Platform {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        for p in Self::ALL {
            if p.as_str() == s {
                return Ok(*p);
            }
        }
        Err(serde::de::Error::custom(format!(
            "unknown platform '{s}', expected one of: {}",
            Self::ALL
                .iter()
                .map(|p| p.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PlatformDownload {
    /// URL to download the artifact from
    pub url: String,
    /// SHA-256 hash of the downloaded file
    pub sha256: String,
    /// Path to the binary inside the archive (for .tar.gz, .zip).
    /// If not set, the downloaded file is treated as the binary itself.
    pub path: Option<String>,
    /// Optional setup command to run after download/extraction.
    /// Executed with cwd set to the cache directory for this artifact.
    /// Only runs once — don writes a marker file after successful setup.
    pub setup: Option<Command>,
}

/// Default base cache directory: .don/cache (project-local)
fn default_cache_base() -> PathBuf {
    PathBuf::from(".don").join("cache")
}

impl DownloadConfig {
    /// Get the download artifact for a specific platform.
    pub fn for_platform(&self, platform: Platform) -> Option<&PlatformDownload> {
        self.platform.get(&platform)
    }
}

impl PlatformDownload {
    /// The directory where this artifact is cached: `<cache_base>/<sha256>/`
    pub fn cache_dir(&self, cache_base: &std::path::Path) -> PathBuf {
        cache_base.join(&self.sha256)
    }

    /// The full path to the downloaded binary.
    ///
    /// - If `path` is set (archive): `<cache_base>/<sha256>/<path>`
    /// - If `path` is not set (bare binary): `<cache_base>/<sha256>/<filename from url>`
    ///
    /// Returns `None` if the URL has no path component (shouldn't happen with valid URLs,
    /// but we don't panic on bad input).
    pub fn binary_path(&self, cache_base: &std::path::Path) -> Option<PathBuf> {
        let dir = self.cache_dir(cache_base);
        match &self.path {
            Some(p) => Some(dir.join(p)),
            None => {
                let filename = self.url.rsplit('/').next().filter(|s| !s.is_empty())?;
                Some(dir.join(filename))
            }
        }
    }
}

/// The resolved preset for a service, after validation.
#[derive(Debug)]
pub enum Preset<'a> {
    Docker(&'a DockerConfig),
    Rust(&'a RustConfig),
    Custom {
        run: &'a Command,
        build: Option<&'a Command>,
    },
}

fn resolve_preset<'a>(
    docker: &'a Option<DockerConfig>,
    rust: &'a Option<RustConfig>,
    run: &'a Option<Command>,
    build: &'a Option<Command>,
) -> Result<Preset<'a>, String> {
    match (docker, rust, run) {
        (Some(docker), None, None) => Ok(Preset::Docker(docker)),
        (None, Some(rust), None) => Ok(Preset::Rust(rust)),
        (None, None, Some(run)) => Ok(Preset::Custom {
            run,
            build: build.as_ref(),
        }),
        (None, None, None) => Err("service must have one of: docker, rust, or run".to_string()),
        _ => Err("service must have only one of: docker, rust, or run".to_string()),
    }
}

impl Service {
    /// Resolve which preset the base service uses (ignoring platform overrides).
    pub fn preset(&self) -> Result<Preset<'_>, String> {
        resolve_preset(&self.docker, &self.rust, &self.run, &self.build)
    }

    /// Resolve the service for a specific platform, applying overrides if present.
    pub fn resolve(&self, platform: Platform) -> ResolvedService {
        match self.platform.get(&platform) {
            None => ResolvedService {
                dir: self.dir.clone(),
                env: self.env.clone(),
                env_file: self.env_file.clone(),
                watch: self.watch.clone(),
                debounce: self.debounce.clone(),
                depends_on: self.depends_on.clone(),
                listen: self.listen.clone(),
                download: self.download.clone(),
                ready: self.ready.clone(),
                shutdown: self.shutdown.clone(),
                log: self.log.clone(),
                docker: self.docker.clone(),
                rust: self.rust.clone(),
                run: self.run.clone(),
                build: self.build.clone(),
            },
            Some(ov) => {
                let mut env = self.env.clone();
                env.extend(ov.env.clone());

                let has_preset_override =
                    ov.docker.is_some() || ov.rust.is_some() || ov.run.is_some();

                let (docker, rust, run, build) = if has_preset_override {
                    (
                        ov.docker.clone(),
                        ov.rust.clone(),
                        ov.run.clone(),
                        ov.build.clone(),
                    )
                } else {
                    (
                        self.docker.clone(),
                        self.rust.clone(),
                        self.run.clone(),
                        ov.build.clone().or_else(|| self.build.clone()),
                    )
                };

                ResolvedService {
                    dir: ov.dir.clone().or_else(|| self.dir.clone()),
                    env,
                    env_file: ov
                        .env_file
                        .clone()
                        .unwrap_or_else(|| self.env_file.clone()),
                    watch: ov.watch.clone().unwrap_or_else(|| self.watch.clone()),
                    debounce: ov.debounce.clone().or_else(|| self.debounce.clone()),
                    depends_on: ov
                        .depends_on
                        .clone()
                        .unwrap_or_else(|| self.depends_on.clone()),
                    listen: ov.listen.clone().unwrap_or_else(|| self.listen.clone()),
                    download: ov.download.clone().or_else(|| self.download.clone()),
                    ready: ov.ready.clone().or_else(|| self.ready.clone()),
                    shutdown: ov.shutdown.clone().or_else(|| self.shutdown.clone()),
                    log: ov.log.clone().unwrap_or_else(|| self.log.clone()),
                    docker,
                    rust,
                    run,
                    build,
                }
            }
        }
    }
}

impl ResolvedService {
    pub fn preset(&self) -> Result<Preset<'_>, String> {
        resolve_preset(&self.docker, &self.rust, &self.run, &self.build)
    }

    /// Resolve the run command for a custom service, taking downloads into account.
    ///
    /// If a download exists for this platform, the binary path from the download
    /// replaces `run.cmd`. The original `run.args` are preserved.
    /// Returns `(executable_path, args)`.
    pub fn resolved_run_cmd(
        &self,
        platform: Platform,
        cache_base: Option<&std::path::Path>,
    ) -> Result<(PathBuf, &[String]), String> {
        let run = self.run.as_ref().ok_or("service has no run command")?;

        let cache_base = cache_base
            .map(PathBuf::from)
            .unwrap_or_else(default_cache_base);

        let executable = match &self.download {
            Some(dl) => match dl.for_platform(platform) {
                Some(artifact) => artifact
                    .binary_path(&cache_base)
                    .ok_or_else(|| format!("download url has no filename: {}", artifact.url))?,
                None => PathBuf::from(&run.cmd),
            },
            None => PathBuf::from(&run.cmd),
        };

        Ok((executable, &run.args))
    }
}

impl std::str::FromStr for Config {
    type Err = toml::de::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        toml::from_str(s)
    }
}

impl Config {
    pub fn from_file(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        Ok(content.parse()?)
    }

    /// All known names (services + tasks) for dependency validation.
    fn all_names(&self) -> std::collections::HashSet<&str> {
        self.services
            .keys()
            .chain(self.tasks.keys())
            .map(|s| s.as_str())
            .collect()
    }

    /// Validate the entire config for a given platform.
    pub fn validate(&self, platform: Platform) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        let all_names = self.all_names();

        // Check for name collisions between services and tasks
        for name in self.services.keys() {
            if self.tasks.contains_key(name) {
                errors.push(format!(
                    "'{name}' is defined as both a service and a task"
                ));
            }
        }

        // Validate services
        for (name, svc) in &self.services {
            let resolved = svc.resolve(platform);
            if let Err(e) = resolved.preset() {
                errors.push(format!("service '{name}': {e}"));
            }
            if let Some(ref ready) = resolved.ready {
                let check_count = ready.exec.is_some() as u8
                    + ready.tcp.is_some() as u8
                    + ready.http.is_some() as u8;
                if check_count == 0 {
                    errors.push(format!(
                        "service '{name}': ready check must have one of: exec, tcp, or http"
                    ));
                } else if check_count > 1 {
                    errors.push(format!(
                        "service '{name}': ready check must have only one of: exec, tcp, or http"
                    ));
                }
            }
            for dep in &resolved.depends_on {
                if !all_names.contains(dep.as_str()) {
                    errors.push(format!(
                        "service '{name}': depends on unknown service or task '{dep}'"
                    ));
                }
            }
        }

        // Validate tasks
        for (name, task) in &self.tasks {
            for dep in &task.depends_on {
                if !all_names.contains(dep.as_str()) {
                    errors.push(format!(
                        "task '{name}': depends on unknown service or task '{dep}'"
                    ));
                }
            }
        }

        // Validate profiles
        for (name, profile) in &self.profiles {
            for svc in &profile.services {
                if !self.services.contains_key(svc) {
                    errors.push(format!(
                        "profile '{name}': references unknown service '{svc}'"
                    ));
                }
            }
            for task in &profile.tasks {
                if !self.tasks.contains_key(task) {
                    errors.push(format!(
                        "profile '{name}': references unknown task '{task}'"
                    ));
                }
            }
        }

        // Detect dependency cycles
        if let Some(cycle) = self.detect_cycle(platform) {
            errors.push(format!("dependency cycle: {}", cycle.join(" -> ")));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Detect dependency cycles using DFS. Returns the cycle path if one exists.
    fn detect_cycle(&self, platform: Platform) -> Option<Vec<String>> {
        // Build adjacency list: name -> list of dependencies (all owned)
        let mut deps: HashMap<String, Vec<String>> = HashMap::new();
        for (name, svc) in &self.services {
            let resolved = svc.resolve(platform);
            deps.insert(name.clone(), resolved.depends_on);
        }
        for (name, task) in &self.tasks {
            deps.insert(name.clone(), task.depends_on.clone());
        }

        #[derive(Clone, Copy, PartialEq)]
        enum State {
            Unvisited,
            Visiting,
            Visited,
        }

        let mut state: HashMap<String, State> =
            deps.keys().map(|k| (k.clone(), State::Unvisited)).collect();
        let mut path: Vec<String> = Vec::new();

        fn dfs(
            node: &str,
            deps: &HashMap<String, Vec<String>>,
            state: &mut HashMap<String, State>,
            path: &mut Vec<String>,
        ) -> Option<Vec<String>> {
            state.insert(node.to_string(), State::Visiting);
            path.push(node.to_string());

            if let Some(neighbors) = deps.get(node) {
                for dep in neighbors {
                    match state.get(dep.as_str()) {
                        Some(State::Visiting) => {
                            let cycle_start = path.iter().position(|n| n == dep).unwrap();
                            let mut cycle: Vec<String> = path[cycle_start..].to_vec();
                            cycle.push(dep.clone());
                            return Some(cycle);
                        }
                        Some(State::Unvisited) | None => {
                            if let Some(cycle) = dfs(dep, deps, state, path) {
                                return Some(cycle);
                            }
                        }
                        Some(State::Visited) => {}
                    }
                }
            }

            path.pop();
            state.insert(node.to_string(), State::Visited);
            None
        }

        let all_nodes: Vec<String> = deps.keys().cloned().collect();
        for node in &all_nodes {
            if state.get(node) == Some(&State::Unvisited)
                && let Some(cycle) = dfs(node, &deps, &mut state, &mut path)
            {
                return Some(cycle);
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_PLATFORM: Platform = Platform::LinuxX86_64;

    #[derive(Debug)]
    struct ConfigTestCase {
        name: &'static str,
        input: &'static str,
        expect_err: bool,
        check: fn(&Config),
    }

    #[test]
    fn test_config_parsing() {
        let cases = vec![
            ConfigTestCase {
                name: "docker service with all fields",
                input: r#"
                    [services.postgres]
                    dir = "/data"
                    docker.image = "postgres:16"
                    docker.container = "my-postgres"
                    docker.ports = ["5432:5432"]
                    docker.volumes = ["pgdata:/var/lib/postgresql/data"]
                    docker.network = "my-net"
                    docker.command = ["postgres", "-c", "max_connections=200"]
                    docker.env_file = [".env.postgres.docker"]
                    env = { POSTGRES_PASSWORD = "dev" }
                    env_file = [".env.shared"]

                    [services.postgres.ready]
                    exec.cmd = "pg_isready"
                    exec.args = ["-h", "localhost"]
                    interval = "500ms"
                    retries = 60

                    [services.postgres.shutdown]
                    signal = "SIGINT"
                    timeout = "30s"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["postgres"].resolve(TEST_PLATFORM);
                    assert_eq!(resolved.dir.as_deref(), Some(std::path::Path::new("/data")));
                    let Preset::Docker(docker) = resolved.preset().unwrap() else {
                        panic!("expected docker preset");
                    };
                    assert_eq!(docker.image, "postgres:16");
                    assert_eq!(docker.container.as_deref(), Some("my-postgres"));
                    assert_eq!(docker.ports, vec!["5432:5432"]);
                    assert_eq!(docker.volumes, vec!["pgdata:/var/lib/postgresql/data"]);
                    assert_eq!(docker.network.as_deref(), Some("my-net"));
                    assert_eq!(docker.command, vec!["postgres", "-c", "max_connections=200"]);
                    assert_eq!(docker.env_file, vec![PathBuf::from(".env.postgres.docker")]);
                    assert_eq!(resolved.env["POSTGRES_PASSWORD"], "dev");
                    assert_eq!(resolved.env_file, vec![PathBuf::from(".env.shared")]);

                    let ready = resolved.ready.as_ref().unwrap();
                    let exec = ready.exec.as_ref().unwrap();
                    assert_eq!(exec.cmd, "pg_isready");
                    assert_eq!(exec.args, vec!["-h", "localhost"]);
                    assert_eq!(ready.interval, "500ms");
                    assert_eq!(ready.retries, 60);

                    let shutdown = resolved.shutdown.as_ref().unwrap();
                    assert_eq!(shutdown.signal, "SIGINT");
                    assert_eq!(shutdown.timeout, "30s");
                },
            },
            ConfigTestCase {
                name: "docker service with build",
                input: r#"
                    [services.api]
                    docker.image = "myapp:dev"
                    docker.ports = ["3000:3000"]
                    docker.build.context = "./services/api"
                    docker.build.dockerfile = "Dockerfile.dev"
                    docker.build.target = "development"
                    docker.build.args = { RUST_VERSION = "1.80" }
                    watch = ["services/api/src/**/*.rs", "services/api/Dockerfile.dev"]
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["api"].resolve(TEST_PLATFORM);
                    let Preset::Docker(docker) = resolved.preset().unwrap() else {
                        panic!("expected docker preset");
                    };
                    assert_eq!(docker.image, "myapp:dev");
                    let build = docker.build.as_ref().unwrap();
                    assert_eq!(build.context, "./services/api");
                    assert_eq!(build.dockerfile.as_deref(), Some("Dockerfile.dev"));
                    assert_eq!(build.target.as_deref(), Some("development"));
                    assert_eq!(build.args["RUST_VERSION"], "1.80");
                    assert_eq!(resolved.watch, vec![
                        "services/api/src/**/*.rs",
                        "services/api/Dockerfile.dev",
                    ]);
                },
            },
            ConfigTestCase {
                name: "rust service with all fields",
                input: r#"
                    [services.api]
                    dir = "./api"
                    rust.binary = "api-server"
                    rust.features = ["dev"]
                    rust.release = true
                    rust.extra_args = ["--jobs", "4"]
                    rust.target_dir = "./target-api"
                    depends_on = ["postgres"]
                    listen = ["0.0.0.0:3000"]

                    [services.api.ready]
                    http = "http://localhost:3000/healthz"

                    [services.api.shutdown]
                    timeout = "5s"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["api"].resolve(TEST_PLATFORM);
                    assert_eq!(resolved.dir.as_deref(), Some(std::path::Path::new("./api")));
                    let Preset::Rust(rust) = resolved.preset().unwrap() else {
                        panic!("expected rust preset");
                    };
                    assert_eq!(rust.binary, "api-server");
                    assert_eq!(rust.features, vec!["dev"]);
                    assert!(rust.release);
                    assert_eq!(rust.extra_args, vec!["--jobs", "4"]);
                    assert_eq!(rust.target_dir.as_deref(), Some(std::path::Path::new("./target-api")));
                    assert_eq!(resolved.depends_on, vec!["postgres"]);
                    assert_eq!(resolved.listen, vec!["0.0.0.0:3000"]);

                    let ready = resolved.ready.as_ref().unwrap();
                    assert_eq!(ready.http.as_deref(), Some("http://localhost:3000/healthz"));
                    assert!(ready.exec.is_none());
                    assert!(ready.tcp.is_none());
                    // Check defaults
                    assert_eq!(ready.interval, "1s");
                    assert_eq!(ready.retries, 30);

                    let shutdown = resolved.shutdown.as_ref().unwrap();
                    assert_eq!(shutdown.signal, "SIGTERM"); // default
                    assert_eq!(shutdown.timeout, "5s");
                },
            },
            ConfigTestCase {
                name: "custom service with cmd and args",
                input: r#"
                    [services.worker]
                    dir = "./worker"
                    run.cmd = "node"
                    run.args = ["worker.js"]
                    build.cmd = "npm"
                    build.args = ["run", "build"]
                    watch = ["src/**/*.js"]

                    [services.worker.ready]
                    tcp = "localhost:9090"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["worker"].resolve(TEST_PLATFORM);
                    let Preset::Custom { run, build } = resolved.preset().unwrap() else {
                        panic!("expected custom preset");
                    };
                    assert_eq!(run.cmd, "node");
                    assert_eq!(run.args, vec!["worker.js"]);
                    let build = build.unwrap();
                    assert_eq!(build.cmd, "npm");
                    assert_eq!(build.args, vec!["run", "build"]);
                    assert_eq!(resolved.dir.as_deref(), Some(std::path::Path::new("./worker")));
                    assert_eq!(resolved.watch, vec!["src/**/*.js"]);

                    let ready = resolved.ready.as_ref().unwrap();
                    assert_eq!(ready.tcp.as_deref(), Some("localhost:9090"));
                },
            },
            ConfigTestCase {
                name: "custom service with no args",
                input: r#"
                    [services.simple]
                    run.cmd = "/usr/bin/myservice"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["simple"].resolve(TEST_PLATFORM);
                    let Preset::Custom { run, build } = resolved.preset().unwrap() else {
                        panic!("expected custom preset");
                    };
                    assert_eq!(run.cmd, "/usr/bin/myservice");
                    assert!(run.args.is_empty());
                    assert!(build.is_none());
                    assert!(resolved.ready.is_none());
                    assert!(resolved.shutdown.is_none());
                },
            },
            ConfigTestCase {
                name: "custom service with download and setup",
                input: r#"
                    [services.crdb]
                    run.cmd = "cockroach"
                    run.args = ["start-single-node", "--insecure"]

                    [services.crdb.download.platform.linux-x86_64]
                    url = "https://binaries.cockroachdb.com/cockroach-v24.1.0.linux-amd64.tgz"
                    sha256 = "abcdef1234567890"
                    path = "cockroach-v24.1.0.linux-amd64/cockroach"

                    [services.crdb.download.platform.macos-aarch64]
                    url = "https://binaries.cockroachdb.com/cockroach-v24.1.0.darwin-11.0-arm64.tgz"
                    sha256 = "fedcba0987654321"
                    path = "cockroach-v24.1.0.darwin-11.0-arm64/cockroach"
                    setup.cmd = "chmod"
                    setup.args = ["+x", "cockroach-v24.1.0.darwin-11.0-arm64/cockroach"]
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["crdb"].resolve(TEST_PLATFORM);
                    let download = resolved.download.as_ref().unwrap();
                    assert_eq!(download.platform.len(), 2);

                    let linux = &download.platform[&Platform::LinuxX86_64];
                    assert_eq!(linux.sha256, "abcdef1234567890");
                    assert!(linux.setup.is_none());

                    let macos = &download.platform[&Platform::MacosAarch64];
                    let setup = macos.setup.as_ref().unwrap();
                    assert_eq!(setup.cmd, "chmod");
                },
            },
            ConfigTestCase {
                name: "platform override switches preset to docker",
                input: r#"
                    [services.crdb]
                    run.cmd = "cockroach"
                    run.args = ["start-single-node", "--insecure"]
                    env = { COCKROACH_PORT = "26257" }

                    [services.crdb.download.platform.linux-x86_64]
                    url = "https://binaries.cockroachdb.com/cockroach-v24.1.0.linux-amd64.tgz"
                    sha256 = "abcdef1234567890"
                    path = "cockroach-v24.1.0.linux-amd64/cockroach"

                    [services.crdb.platform.macos-aarch64]
                    docker.image = "cockroachdb/cockroach:v24.1.0"
                    docker.ports = ["26257:26257"]
                "#,
                expect_err: false,
                check: |config| {
                    let linux = config.services["crdb"].resolve(Platform::LinuxX86_64);
                    let Preset::Custom { run, .. } = linux.preset().unwrap() else {
                        panic!("expected custom preset on linux");
                    };
                    assert_eq!(run.cmd, "cockroach");
                    assert!(linux.download.is_some());
                    assert_eq!(linux.env["COCKROACH_PORT"], "26257");

                    let macos = config.services["crdb"].resolve(Platform::MacosAarch64);
                    let Preset::Docker(docker) = macos.preset().unwrap() else {
                        panic!("expected docker preset on macos");
                    };
                    assert_eq!(docker.image, "cockroachdb/cockroach:v24.1.0");
                    assert_eq!(macos.env["COCKROACH_PORT"], "26257");
                    assert!(macos.download.is_some());
                },
            },
            ConfigTestCase {
                name: "platform override merges env",
                input: r#"
                    [services.api]
                    rust.binary = "api-server"
                    env = { PORT = "3000", LOG_LEVEL = "info" }

                    [services.api.platform.linux-x86_64]
                    env = { LOG_LEVEL = "debug", EXTRA = "linux-only" }
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["api"].resolve(Platform::LinuxX86_64);
                    assert_eq!(resolved.env["PORT"], "3000");
                    assert_eq!(resolved.env["LOG_LEVEL"], "debug");
                    assert_eq!(resolved.env["EXTRA"], "linux-only");
                },
            },
            ConfigTestCase {
                name: "platform override replaces watch list",
                input: r#"
                    [services.api]
                    rust.binary = "api-server"
                    watch = ["src/**/*.rs"]

                    [services.api.platform.linux-x86_64]
                    watch = ["src/**/*.rs", "config/**/*.toml"]
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["api"].resolve(Platform::LinuxX86_64);
                    assert_eq!(resolved.watch, vec!["src/**/*.rs", "config/**/*.toml"]);

                    let other = config.services["api"].resolve(Platform::MacosAarch64);
                    assert_eq!(other.watch, vec!["src/**/*.rs"]);
                },
            },
            ConfigTestCase {
                name: "no base preset but valid via platform override",
                input: r#"
                    [services.crdb]
                    env = { PORT = "26257" }

                    [services.crdb.platform.linux-x86_64]
                    run.cmd = "cockroach"
                    run.args = ["start-single-node", "--insecure"]

                    [services.crdb.platform.macos-aarch64]
                    docker.image = "cockroachdb/cockroach:v24.1.0"
                "#,
                expect_err: false,
                check: |config| {
                    let linux = config.services["crdb"].resolve(Platform::LinuxX86_64);
                    let Preset::Custom { run, .. } = linux.preset().unwrap() else {
                        panic!("expected custom on linux");
                    };
                    assert_eq!(run.cmd, "cockroach");

                    let macos = config.services["crdb"].resolve(Platform::MacosAarch64);
                    let Preset::Docker(docker) = macos.preset().unwrap() else {
                        panic!("expected docker on macos");
                    };
                    assert_eq!(docker.image, "cockroachdb/cockroach:v24.1.0");

                    let windows = config.services["crdb"].resolve(Platform::WindowsX86_64);
                    assert!(windows.preset().is_err());
                },
            },
            ConfigTestCase {
                name: "invalid platform key",
                input: r#"
                    [services.bad]
                    run.cmd = "./bad"

                    [services.bad.download.platform.ubuntu-amd64]
                    url = "https://example.com/bad"
                    sha256 = "bad"
                "#,
                expect_err: true,
                check: |_| {},
            },
            ConfigTestCase {
                name: "log defaults to stdout",
                input: r#"
                    [services.svc]
                    run.cmd = "mybin"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["svc"].resolve(TEST_PLATFORM);
                    assert!(matches!(resolved.log, LogConfig::Stdout));
                },
            },
            ConfigTestCase {
                name: "log ignore",
                input: r#"
                    [services.svc]
                    run.cmd = "mybin"
                    log = "ignore"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["svc"].resolve(TEST_PLATFORM);
                    assert!(matches!(resolved.log, LogConfig::Ignore));
                },
            },
            ConfigTestCase {
                name: "log to file via string",
                input: r#"
                    [services.svc]
                    run.cmd = "mybin"
                    log = "logs/mybin.log"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["svc"].resolve(TEST_PLATFORM);
                    let LogConfig::File(path) = &resolved.log else {
                        panic!("expected file log config");
                    };
                    assert_eq!(path, &PathBuf::from("logs/mybin.log"));
                },
            },
            ConfigTestCase {
                name: "log to file via table",
                input: r#"
                    [services.svc]
                    run.cmd = "mybin"
                    log.file = "logs/mybin.log"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["svc"].resolve(TEST_PLATFORM);
                    let LogConfig::File(path) = &resolved.log else {
                        panic!("expected file log config");
                    };
                    assert_eq!(path, &PathBuf::from("logs/mybin.log"));
                },
            },
            ConfigTestCase {
                name: "log explicit stdout",
                input: r#"
                    [services.svc]
                    run.cmd = "mybin"
                    log = "stdout"
                "#,
                expect_err: false,
                check: |config| {
                    let resolved = config.services["svc"].resolve(TEST_PLATFORM);
                    assert!(matches!(resolved.log, LogConfig::Stdout));
                },
            },
            ConfigTestCase {
                name: "task with file watching",
                input: r#"
                    [services.postgres]
                    docker.image = "postgres:16"
                    [services.postgres.ready]
                    tcp = "localhost:5432"

                    [tasks.migrate]
                    cmd = "dbmate"
                    args = ["up"]
                    depends_on = ["postgres"]
                    watch = ["db/migrations/**/*.sql"]
                    dir = "./db"
                    env = { DATABASE_URL = "postgres://localhost:5432/dev" }

                    [tasks.seed]
                    cmd = "psql"
                    args = ["-f", "seed.sql"]
                    depends_on = ["migrate"]
                    watch = ["db/seed.sql"]
                    log = "ignore"
                "#,
                expect_err: false,
                check: |config| {
                    assert_eq!(config.tasks.len(), 2);

                    let migrate = &config.tasks["migrate"];
                    assert_eq!(migrate.cmd, "dbmate");
                    assert_eq!(migrate.args, vec!["up"]);
                    assert_eq!(migrate.depends_on, vec!["postgres"]);
                    assert_eq!(migrate.watch, vec!["db/migrations/**/*.sql"]);
                    assert_eq!(migrate.dir.as_deref(), Some(std::path::Path::new("./db")));
                    assert_eq!(migrate.env["DATABASE_URL"], "postgres://localhost:5432/dev");

                    let seed = &config.tasks["seed"];
                    assert_eq!(seed.depends_on, vec!["migrate"]);
                    assert!(matches!(seed.log, LogConfig::Ignore));

                    assert!(config.validate(TEST_PLATFORM).is_ok());
                },
            },
            ConfigTestCase {
                name: "task with no watch always runs",
                input: r#"
                    [tasks.setup]
                    cmd = "echo"
                    args = ["hello"]
                "#,
                expect_err: false,
                check: |config| {
                    let task = &config.tasks["setup"];
                    assert!(task.watch.is_empty());
                    assert!(task.depends_on.is_empty());
                    assert!(config.validate(TEST_PLATFORM).is_ok());
                },
            },
            ConfigTestCase {
                name: "service depends on task",
                input: r#"
                    [tasks.migrate]
                    cmd = "dbmate"
                    args = ["up"]

                    [services.api]
                    rust.binary = "api-server"
                    depends_on = ["migrate"]
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_ok());
                },
            },
            ConfigTestCase {
                name: "task depends on unknown name is a validation error",
                input: r#"
                    [tasks.migrate]
                    cmd = "dbmate"
                    args = ["up"]
                    depends_on = ["nonexistent"]
                "#,
                expect_err: false,
                check: |config| {
                    let errors = config.validate(TEST_PLATFORM).unwrap_err();
                    assert!(errors[0].contains("unknown service or task 'nonexistent'"));
                },
            },
            ConfigTestCase {
                name: "service and task with same name is a validation error",
                input: r#"
                    [services.foo]
                    run.cmd = "foo"

                    [tasks.foo]
                    cmd = "foo"
                "#,
                expect_err: false,
                check: |config| {
                    let errors = config.validate(TEST_PLATFORM).unwrap_err();
                    assert!(errors[0].contains("both a service and a task"));
                },
            },
            ConfigTestCase {
                name: "service depends on unknown name is a validation error",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    depends_on = ["ghost"]
                "#,
                expect_err: false,
                check: |config| {
                    let errors = config.validate(TEST_PLATFORM).unwrap_err();
                    assert!(errors[0].contains("unknown service or task 'ghost'"));
                },
            },
            ConfigTestCase {
                name: "dependency cycle is a validation error",
                input: r#"
                    [services.a]
                    run.cmd = "a"
                    depends_on = ["b"]

                    [services.b]
                    run.cmd = "b"
                    depends_on = ["c"]

                    [tasks.c]
                    cmd = "c"
                    depends_on = ["a"]
                "#,
                expect_err: false,
                check: |config| {
                    let errors = config.validate(TEST_PLATFORM).unwrap_err();
                    assert!(errors.iter().any(|e| e.contains("dependency cycle")));
                },
            },
            ConfigTestCase {
                name: "self-referencing dependency is a cycle",
                input: r#"
                    [services.loop]
                    run.cmd = "loop"
                    depends_on = ["loop"]
                "#,
                expect_err: false,
                check: |config| {
                    let errors = config.validate(TEST_PLATFORM).unwrap_err();
                    assert!(errors.iter().any(|e| e.contains("dependency cycle")));
                },
            },
            ConfigTestCase {
                name: "profiles with valid references",
                input: r#"
                    [services.api]
                    rust.binary = "api"

                    [services.postgres]
                    docker.image = "postgres:16"

                    [tasks.migrate]
                    cmd = "dbmate"
                    args = ["up"]

                    [profiles.frontend]
                    services = ["api", "postgres"]
                    tasks = ["migrate"]
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_ok());
                    let profile = &config.profiles["frontend"];
                    assert_eq!(profile.services, vec!["api", "postgres"]);
                    assert_eq!(profile.tasks, vec!["migrate"]);
                },
            },
            ConfigTestCase {
                name: "profile with unknown service is a validation error",
                input: r#"
                    [services.api]
                    rust.binary = "api"

                    [profiles.bad]
                    services = ["api", "nonexistent"]
                "#,
                expect_err: false,
                check: |config| {
                    let errors = config.validate(TEST_PLATFORM).unwrap_err();
                    assert!(errors[0].contains("unknown service 'nonexistent'"));
                },
            },
            ConfigTestCase {
                name: "profile with unknown task is a validation error",
                input: r#"
                    [services.api]
                    rust.binary = "api"

                    [profiles.bad]
                    tasks = ["ghost"]
                "#,
                expect_err: false,
                check: |config| {
                    let errors = config.validate(TEST_PLATFORM).unwrap_err();
                    assert!(errors[0].contains("unknown task 'ghost'"));
                },
            },
            ConfigTestCase {
                name: "task with timeout",
                input: r#"
                    [tasks.slow]
                    cmd = "make"
                    args = ["build"]
                    timeout = "5m"
                "#,
                expect_err: false,
                check: |config| {
                    let task = &config.tasks["slow"];
                    assert_eq!(task.timeout.as_deref(), Some("5m"));
                },
            },
            ConfigTestCase {
                name: "empty config",
                input: "",
                expect_err: false,
                check: |config| {
                    assert!(config.services.is_empty());
                },
            },
            ConfigTestCase {
                name: "no preset is a validation error",
                input: r#"
                    [services.broken]
                    env = { FOO = "bar" }
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_err());
                },
            },
            ConfigTestCase {
                name: "conflicting presets is a validation error",
                input: r#"
                    [services.broken]
                    docker.image = "postgres:16"
                    run.cmd = "something"
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_err());
                },
            },
            ConfigTestCase {
                name: "ready check with no check type is a validation error",
                input: r#"
                    [services.broken]
                    run.cmd = "myservice"
                    [services.broken.ready]
                    interval = "1s"
                "#,
                expect_err: false,
                check: |config| {
                    let errors = config.validate(TEST_PLATFORM).unwrap_err();
                    assert!(errors[0].contains("ready check must have one of"));
                },
            },
            ConfigTestCase {
                name: "ready check with multiple check types is a validation error",
                input: r#"
                    [services.broken]
                    run.cmd = "myservice"
                    [services.broken.ready]
                    tcp = "localhost:8080"
                    http = "http://localhost:8080/health"
                "#,
                expect_err: false,
                check: |config| {
                    let errors = config.validate(TEST_PLATFORM).unwrap_err();
                    assert!(errors[0].contains("ready check must have only one of"));
                },
            },
        ];

        for case in &cases {
            let result = case.input.parse::<Config>();
            if case.expect_err {
                assert!(
                    result.is_err(),
                    "case '{}': expected parse error",
                    case.name
                );
                continue;
            }
            let config = result
                .unwrap_or_else(|e| panic!("case '{}': unexpected error: {e}", case.name));
            (case.check)(&config);
        }
    }

    #[test]
    fn test_resolved_run_cmd() {
        struct RunCmdTestCase {
            name: &'static str,
            input: &'static str,
            platform: Platform,
            cache_base: &'static str,
            expect_executable: &'static str,
            expect_args: &'static [&'static str],
        }

        let cases = vec![
            RunCmdTestCase {
                name: "no download uses cmd directly",
                input: r#"
                    [services.svc]
                    run.cmd = "mybin"
                    run.args = ["--port", "8080"]
                "#,
                platform: Platform::LinuxX86_64,
                cache_base: "/tmp/don-cache",
                expect_executable: "mybin",
                expect_args: &["--port", "8080"],
            },
            RunCmdTestCase {
                name: "download with archive path resolves to cache",
                input: r#"
                    [services.svc]
                    run.cmd = "cockroach"
                    run.args = ["start-single-node", "--insecure"]

                    [services.svc.download.platform.linux-x86_64]
                    url = "https://example.com/cockroach-v24.tgz"
                    sha256 = "abc123"
                    path = "cockroach-v24/cockroach"
                "#,
                platform: Platform::LinuxX86_64,
                cache_base: "/tmp/don-cache",
                expect_executable: "/tmp/don-cache/abc123/cockroach-v24/cockroach",
                expect_args: &["start-single-node", "--insecure"],
            },
            RunCmdTestCase {
                name: "download without archive path uses url filename",
                input: r#"
                    [services.svc]
                    run.cmd = "mytool"
                    run.args = ["serve"]

                    [services.svc.download.platform.linux-x86_64]
                    url = "https://example.com/releases/mytool-linux-amd64"
                    sha256 = "def456"
                "#,
                platform: Platform::LinuxX86_64,
                cache_base: "/tmp/don-cache",
                expect_executable: "/tmp/don-cache/def456/mytool-linux-amd64",
                expect_args: &["serve"],
            },
            RunCmdTestCase {
                name: "no download for this platform falls back to cmd",
                input: r#"
                    [services.svc]
                    run.cmd = "cockroach"
                    run.args = ["start"]

                    [services.svc.download.platform.linux-x86_64]
                    url = "https://example.com/cockroach-linux.tgz"
                    sha256 = "abc123"
                    path = "cockroach"
                "#,
                platform: Platform::MacosAarch64,
                cache_base: "/tmp/don-cache",
                expect_executable: "cockroach",
                expect_args: &["start"],
            },
        ];

        for case in &cases {
            let config = case.input.parse::<Config>()
                .unwrap_or_else(|e| panic!("case '{}': parse error: {e}", case.name));
            let resolved = config.services["svc"].resolve(case.platform);
            let (executable, args) = resolved
                .resolved_run_cmd(case.platform, Some(std::path::Path::new(case.cache_base)))
                .unwrap_or_else(|e| panic!("case '{}': resolve error: {e}", case.name));

            assert_eq!(
                executable,
                PathBuf::from(case.expect_executable),
                "case '{}': executable mismatch",
                case.name
            );
            let expected_args: Vec<String> =
                case.expect_args.iter().map(|s| s.to_string()).collect();
            assert_eq!(
                args, &expected_args[..],
                "case '{}': args mismatch",
                case.name
            );
        }
    }
}
