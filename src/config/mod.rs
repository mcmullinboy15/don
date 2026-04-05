//! Configuration parsing, validation, and platform resolution for don.
//!
//! The config is loaded from a `don.toml` file and defines services, tasks,
//! and profiles for a dev environment.

mod download;
mod platform;
mod profile;
mod service;
mod task;
mod types;

pub use self::download::{DownloadConfig, PlatformDownload};
pub use self::platform::Platform;
pub use self::profile::Profile;
pub use self::service::{
    DockerBuildConfig, DockerConfig, Preset, ResolvedService, RustConfig, Service,
};
pub use self::task::Task;
pub use self::types::{Command, LogConfig, ReadyCheck, ShutdownConfig};

pub use self::service::ServiceOverride;

use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

/// Top-level don configuration, typically loaded from `don.toml`.
#[derive(Debug, Deserialize)]
pub struct Config {
    /// Long-running services (databases, APIs, workers, etc.).
    #[serde(default)]
    pub services: HashMap<String, Service>,
    /// One-shot tasks (migrations, codegen, etc.).
    /// Only re-run when watched files change since last successful run.
    #[serde(default)]
    pub tasks: HashMap<String, Task>,
    /// Named profiles — subsets of services/tasks to run.
    #[serde(default)]
    pub profiles: HashMap<String, Profile>,
}

impl std::str::FromStr for Config {
    type Err = toml::de::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        toml::from_str(s)
    }
}

const VALID_SIGNALS: &[&str] = &[
    "SIGTERM", "SIGINT", "SIGQUIT", "SIGHUP", "SIGUSR1", "SIGUSR2",
];

fn is_valid_signal(s: &str) -> bool {
    VALID_SIGNALS.contains(&s)
}

impl Config {
    /// Load and parse a config from a file path.
    pub fn from_file(path: &std::path::Path) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path).map_err(|source| ConfigError::ReadFile {
            path: path.to_path_buf(),
            source,
        })?;
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
    ///
    /// Checks preset validity, ready check configuration, dependency references,
    /// profile references, and dependency cycles.
    pub fn validate(&self, platform: Platform) -> Result<Vec<String>, ConfigError> {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        let all_names = self.all_names();

        // Check for name collisions between services and tasks
        for name in self.services.keys() {
            if self.tasks.contains_key(name) {
                errors.push(format!("'{name}' is defined as both a service and a task"));
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
            // Warn if a service with listen addresses uses a TCP ready check —
            // the TCP connect will succeed immediately against don's socket
            // without proving the service is actually accepting connections.
            if !resolved.listen.is_empty()
                && let Some(ref ready) = resolved.ready
                && let Some(ref tcp_addr) = ready.tcp
                && resolved.listen.iter().any(|l| l == tcp_addr)
            {
                warnings.push(format!(
                    "service '{name}': TCP ready check on '{tcp_addr}' will pass \
                     immediately because don holds that socket — use an HTTP or \
                     exec ready check instead"
                ));
            }
            for dep in &resolved.depends_on {
                if !all_names.contains(dep.as_str()) {
                    errors.push(format!(
                        "service '{name}': depends on unknown service or task '{dep}'"
                    ));
                }
            }
            // Validate duration strings
            for pattern in &resolved.watch {
                if let Err(e) = glob::Pattern::new(pattern) {
                    errors.push(format!("service '{name}': invalid watch pattern '{pattern}': {e}"));
                }
            }
            for pattern in &resolved.ignore {
                if let Err(e) = glob::Pattern::new(pattern) {
                    errors.push(format!("service '{name}': invalid ignore pattern '{pattern}': {e}"));
                }
            }
            if let Some(ref debounce) = resolved.debounce
                && let Err(e) = crate::duration::parse_duration(debounce)
            {
                errors.push(format!("service '{name}': invalid debounce: {e}"));
            }
            if let Some(ref ready) = resolved.ready
                && let Err(e) = crate::duration::parse_duration(&ready.interval)
            {
                errors.push(format!("service '{name}': invalid ready interval: {e}"));
            }
            if let Some(ref shutdown) = resolved.shutdown {
                if let Err(e) = crate::duration::parse_duration(&shutdown.timeout) {
                    errors.push(format!("service '{name}': invalid shutdown timeout: {e}"));
                }
                if !is_valid_signal(&shutdown.signal) {
                    errors.push(format!(
                        "service '{name}': unknown shutdown signal '{}' (expected SIGTERM, SIGINT, SIGQUIT, SIGHUP, SIGUSR1, or SIGUSR2)",
                        shutdown.signal
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
            for pattern in &task.watch {
                if let Err(e) = glob::Pattern::new(pattern) {
                    errors.push(format!("task '{name}': invalid watch pattern '{pattern}': {e}"));
                }
            }
            for pattern in &task.ignore {
                if let Err(e) = glob::Pattern::new(pattern) {
                    errors.push(format!("task '{name}': invalid ignore pattern '{pattern}': {e}"));
                }
            }
            if let Some(ref timeout) = task.timeout
                && let Err(e) = crate::duration::parse_duration(timeout)
            {
                errors.push(format!("task '{name}': invalid timeout: {e}"));
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
            Ok(warnings)
        } else {
            Err(ConfigError::Validation { errors })
        }
    }

    /// Detect dependency cycles using DFS. Returns the cycle path if one exists.
    fn detect_cycle(&self, platform: Platform) -> Option<Vec<String>> {
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
                            let cycle_start = path.iter().position(|n| n == dep)?;
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

/// Errors that can occur when loading or validating a don config.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The config file could not be read from disk.
    #[error("failed to read config file '{}': {source}", path.display())]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
    },
    /// The config file contains invalid TOML or doesn't match the expected schema.
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
    /// The config is syntactically valid but contains semantic errors.
    #[error("config validation failed:\n{}", errors.join("\n"))]
    Validation { errors: Vec<String> },
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
                    assert_eq!(ready.interval, "1s");
                    assert_eq!(ready.retries, 30);

                    let shutdown = resolved.shutdown.as_ref().unwrap();
                    assert_eq!(shutdown.signal, "SIGTERM");
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

                    // Platform without an override and no base preset should fail
                    let other = config.services["crdb"].resolve(Platform::LinuxAarch64);
                    assert!(other.preset().is_err());
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
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
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
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
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
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
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
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
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
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
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
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
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
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
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
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
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
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors[0].contains("ready check must have only one of"));
                },
            },
            ConfigTestCase {
                name: "invalid debounce duration is a validation error",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    debounce = "banana"
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors.iter().any(|e| e.contains("invalid debounce")));
                },
            },
            ConfigTestCase {
                name: "invalid ready interval is a validation error",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    [services.api.ready]
                    tcp = "localhost:3000"
                    interval = "nope"
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors.iter().any(|e| e.contains("invalid ready interval")));
                },
            },
            ConfigTestCase {
                name: "invalid shutdown timeout is a validation error",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    [services.api.shutdown]
                    timeout = "forever"
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors.iter().any(|e| e.contains("invalid shutdown timeout")));
                },
            },
            ConfigTestCase {
                name: "invalid task timeout is a validation error",
                input: r#"
                    [tasks.build]
                    cmd = "make"
                    timeout = "lots"
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors.iter().any(|e| e.contains("invalid timeout")));
                },
            },
            ConfigTestCase {
                name: "invalid shutdown signal is a validation error",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    [services.api.shutdown]
                    signal = "SIGBANANA"
                "#,
                expect_err: false,
                check: |config| {
                    let err = config.validate(TEST_PLATFORM).unwrap_err();
                    let ConfigError::Validation { errors } = &err else {
                        panic!("expected validation error");
                    };
                    assert!(errors.iter().any(|e| e.contains("unknown shutdown signal")));
                },
            },
            ConfigTestCase {
                name: "valid shutdown signals pass validation",
                input: r#"
                    [services.api]
                    run.cmd = "api"
                    [services.api.shutdown]
                    signal = "SIGINT"
                "#,
                expect_err: false,
                check: |config| {
                    assert!(config.validate(TEST_PLATFORM).is_ok());
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
