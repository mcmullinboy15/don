use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

use super::download::{DownloadConfig, default_cache_base};
use super::platform::Platform;
use super::types::{Command, LogConfig, ReadyCheck, ShutdownConfig};

/// A long-running service. Uses exactly one preset: docker, rust, or custom (run).
#[derive(Debug, Deserialize)]
pub struct Service {
    /// Working directory for the service. Defaults to the current directory.
    pub dir: Option<PathBuf>,
    /// Environment variables. No env vars are loaded by default.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Paths to env files to load. Don also auto-loads `.env.<service-name>` if it exists.
    #[serde(default)]
    pub env_file: Vec<PathBuf>,
    /// File glob patterns to watch for rebuilding/restarting.
    #[serde(default)]
    pub watch: Vec<String>,
    /// Debounce window for file watch events (e.g. "500ms" or "1s"). Defaults to "200ms".
    pub debounce: Option<String>,
    /// Services that must be started before this one.
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Addresses for don to listen on and pass to the service via LISTEN_FDS.
    /// Don holds the sockets open across restarts so traffic is never dropped.
    #[serde(default)]
    pub listen: Vec<String>,
    /// Optional binary download configuration for this service.
    pub download: Option<DownloadConfig>,
    /// Ready check — used to gate dependents until this service is accepting traffic.
    pub ready: Option<ReadyCheck>,
    /// Shutdown behavior.
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
    /// Command to run the service.
    pub run: Option<Command>,
    /// Command to build the service before running.
    pub build: Option<Command>,
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

/// Docker container configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct DockerConfig {
    /// Docker image to run (e.g. "postgres:16").
    /// For built images, this is also used as the tag for `docker build -t`.
    pub image: String,
    /// Container name — used to check if it's already running.
    pub container: Option<String>,
    /// Port mappings (e.g. ["5432:5432"]).
    #[serde(default)]
    pub ports: Vec<String>,
    /// Volume mounts (e.g. ["pgdata:/var/lib/postgresql/data"]).
    #[serde(default)]
    pub volumes: Vec<String>,
    /// Docker network to attach to.
    pub network: Option<String>,
    /// Override the container's default command / entrypoint args.
    #[serde(default)]
    pub command: Vec<String>,
    /// Env files to pass to docker via --env-file.
    #[serde(default)]
    pub env_file: Vec<PathBuf>,
    /// Build configuration — if set, don builds the image before running.
    pub build: Option<DockerBuildConfig>,
}

/// Docker image build configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct DockerBuildConfig {
    /// Build context path (e.g. "." or "./services/api").
    pub context: String,
    /// Path to the Dockerfile, relative to context. Defaults to "Dockerfile".
    pub dockerfile: Option<String>,
    /// Build target for multi-stage builds (e.g. "development").
    pub target: Option<String>,
    /// Build arguments passed via --build-arg.
    #[serde(default)]
    pub args: HashMap<String, String>,
}

/// Rust/Cargo service configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct RustConfig {
    /// Name of the binary target to build and run.
    pub binary: String,
    /// Cargo features to enable.
    #[serde(default)]
    pub features: Vec<String>,
    /// Build in release mode (default: false).
    #[serde(default)]
    pub release: bool,
    /// Extra arguments to pass to `cargo build`.
    #[serde(default)]
    pub extra_args: Vec<String>,
    /// Override the cargo target directory.
    pub target_dir: Option<PathBuf>,
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

pub(crate) fn resolve_preset<'a>(
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
                    env_file: ov.env_file.clone().unwrap_or_else(|| self.env_file.clone()),
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
    /// Resolve which preset this resolved service uses.
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
