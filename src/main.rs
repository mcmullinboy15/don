// The CLI binary legitimately uses stdout — it IS the user-facing output.
#![allow(clippy::print_stdout)]

use clap::{Parser, Subcommand};
use crossterm::style::{Color, ResetColor, SetForegroundColor};
use don::client::{Client, ClientError};
use don::runner::{ItemStatus, ServiceState, TaskItemState};
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[command(name = "don", about = "Boss of your dev environment")]
struct Cli {
    /// Path to the config file
    #[arg(short, long, default_value = "don.toml", global = true)]
    config: PathBuf,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start services and run tasks
    Start {
        /// Run only services/tasks in this profile
        #[arg(short, long)]
        profile: Option<String>,
        /// Name of a stopped service to start (omit to start the daemon)
        name: Option<String>,
    },
    /// Stop a running service
    Stop {
        /// Name of the service to stop
        name: String,
    },
    /// Restart a running service
    Restart {
        /// Name of the service to restart
        name: String,
    },
    /// Show status of all services and tasks
    Status,
    /// View logs for a service or task
    Logs {
        /// Name of the service or task
        name: String,
        /// Show last N lines
        #[arg(short, long, default_value_t = 100)]
        last: usize,
        /// Follow the log output
        #[arg(short, long)]
        follow: bool,
    },
    /// Interactively attach stdin/stdout to a running service
    Attach {
        /// Name of the service to attach to
        name: String,
    },
    /// Clean up stale state from a previous run
    Cleanup {
        /// Kill a running daemon first, then clean up
        #[arg(long)]
        force: bool,
    },
    /// Validate the config file
    Validate,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let command = cli.command.unwrap_or(Commands::Start {
        profile: None,
        name: None,
    });

    let exit_code = run(cli.config, command).await;
    std::process::exit(exit_code);
}

async fn run(config_path: PathBuf, command: Commands) -> i32 {
    match command {
        Commands::Validate => match validate(&config_path) {
            Ok(()) => {
                println!("Config is valid.");
                0
            }
            Err(e) => {
                eprintln!("{e}");
                1
            }
        },
        Commands::Start { profile, name: None } => match run_start(&config_path, profile.as_deref()).await {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("{e}");
                1
            }
        },
        Commands::Start { name: Some(name), .. } => {
            run_client(&config_path, |c| async move { c.start(&name).await }).await
        }
        Commands::Stop { name } => {
            run_client(&config_path, |c| async move { c.stop(&name).await }).await
        }
        Commands::Restart { name } => {
            run_client(&config_path, |c| async move { c.restart(&name).await }).await
        }
        Commands::Status => run_status(&config_path).await,
        Commands::Logs { name, last, follow } => run_logs(&config_path, &name, last, follow).await,
        Commands::Attach { name } => {
            eprintln!("don attach {name}: not yet implemented (Phase 17)");
            1
        }
        Commands::Cleanup { force } => run_cleanup_command(&config_path, force).await,
    }
}

fn client_for(config_path: &Path) -> Client {
    Client::new(base_dir(config_path).as_path())
}

fn base_dir(config_path: &Path) -> PathBuf {
    match config_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

/// Run a client command that returns `()` on success. Prints a friendly
/// error and returns exit code 1 on failure.
async fn run_client<F, Fut>(config_path: &Path, make_call: F) -> i32
where
    F: FnOnce(Client) -> Fut,
    Fut: std::future::Future<Output = Result<(), ClientError>>,
{
    let client = client_for(config_path);
    match make_call(client).await {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{e}");
            1
        }
    }
}

async fn run_status(config_path: &Path) -> i32 {
    let client = client_for(config_path);
    match client.status().await {
        Ok(items) => {
            print_status_table(&items);
            0
        }
        Err(e) => {
            eprintln!("{e}");
            1
        }
    }
}

async fn run_logs(config_path: &Path, name: &str, last: usize, follow: bool) -> i32 {
    let client = client_for(config_path);
    if follow {
        match client
            .logs_follow(name, last, |line| {
                // Each NDJSON frame is `{"line":"..."}`.
                match serde_json::from_str::<serde_json::Value>(line) {
                    Ok(v) => {
                        if let Some(s) = v.get("line").and_then(|x| x.as_str()) {
                            println!("{s}");
                        }
                    }
                    Err(_) => {
                        // Fall back to raw line — shouldn't happen with this server.
                        println!("{line}");
                    }
                }
                Ok(())
            })
            .await
        {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("{e}");
                1
            }
        }
    } else {
        match client.logs(name, last).await {
            Ok(lines) => {
                for line in lines {
                    println!("{line}");
                }
                0
            }
            Err(e) => {
                eprintln!("{e}");
                1
            }
        }
    }
}

fn print_status_table(items: &[ItemStatus]) {
    if items.is_empty() {
        println!("(no services or tasks)");
        return;
    }
    // Compute column widths.
    let kind_w = "KIND".len().max(
        items
            .iter()
            .map(|i| match i {
                ItemStatus::Service { .. } => "service".len(),
                ItemStatus::Task { .. } => "task".len(),
            })
            .max()
            .unwrap_or(0),
    );
    let name_w = "NAME".len().max(
        items
            .iter()
            .map(|i| match i {
                ItemStatus::Service { name, .. } | ItemStatus::Task { name, .. } => name.len(),
            })
            .max()
            .unwrap_or(0),
    );

    println!("{:<kind_w$}  {:<name_w$}  STATE", "KIND", "NAME");
    for item in items {
        let (kind, name, state_str, color) = match item {
            ItemStatus::Service { name, state } => {
                ("service", name.as_str(), service_state_label(*state), service_state_color(*state))
            }
            ItemStatus::Task { name, state } => {
                ("task", name.as_str(), task_state_label(*state), task_state_color(*state))
            }
        };
        println!(
            "{:<kind_w$}  {:<name_w$}  {}{}{}",
            kind,
            name,
            SetForegroundColor(color),
            state_str,
            ResetColor,
        );
    }
}

fn service_state_label(s: ServiceState) -> &'static str {
    match s {
        ServiceState::Pending => "pending",
        ServiceState::Starting => "starting",
        ServiceState::Running => "running",
        ServiceState::Ready => "ready",
        ServiceState::Stopping => "stopping",
        ServiceState::Stopped => "stopped",
        ServiceState::Failed => "failed",
    }
}

fn service_state_color(s: ServiceState) -> Color {
    match s {
        ServiceState::Ready | ServiceState::Running => Color::Green,
        ServiceState::Starting | ServiceState::Pending | ServiceState::Stopping => Color::Yellow,
        ServiceState::Stopped => Color::DarkGrey,
        ServiceState::Failed => Color::Red,
    }
}

fn task_state_label(s: TaskItemState) -> &'static str {
    match s {
        TaskItemState::Pending => "pending",
        TaskItemState::Running => "running",
        TaskItemState::Completed => "completed",
        TaskItemState::Skipped => "skipped",
        TaskItemState::Failed => "failed",
        TaskItemState::PendingRerun => "pending_rerun",
    }
}

fn task_state_color(s: TaskItemState) -> Color {
    match s {
        TaskItemState::Completed | TaskItemState::Skipped => Color::Green,
        TaskItemState::Running | TaskItemState::Pending => Color::Yellow,
        TaskItemState::PendingRerun => Color::Cyan,
        TaskItemState::Failed => Color::Red,
    }
}

async fn run_cleanup_command(config_path: &std::path::Path, force: bool) -> i32 {
    let base = base_dir(config_path);
    let don_dir = base.join(".don");
    let _ = std::fs::create_dir_all(&don_dir);

    // Acquire the PID file lock so we don't race with a running daemon.
    let don_pid_path = don_dir.join("don.pid");
    let pid_lock = match don::process::pid_file::PidFile::acquire(
        don_pid_path.clone(),
        std::process::id() as i32,
    )
    .await
    {
        Ok(lock) => lock,
        Err(don::process::pid_file::PidFileError::AlreadyLocked) => {
            if !force {
                println!("don daemon is running — nothing to clean up (use --force to kill it)");
                return 0;
            }
            // --force: read the running daemon's PID and kill it.
            eprintln!("killing running don daemon...");
            if let Err(e) = kill_running_daemon(&don_pid_path).await {
                eprintln!("failed to kill daemon: {e}");
                return 1;
            }
            // Now re-acquire the lock.
            match don::process::pid_file::PidFile::acquire(
                don_pid_path,
                std::process::id() as i32,
            )
            .await
            {
                Ok(lock) => lock,
                Err(e) => {
                    eprintln!("failed to acquire pid lock after kill: {e}");
                    return 1;
                }
            }
        }
        Err(e) => {
            eprintln!("failed to acquire pid lock: {e}");
            return 1;
        }
    };

    // Load config to discover docker container names. If config doesn't
    // exist or is invalid, still clean up what we can (pid files and socket).
    let docker_names: Vec<String> = match don::config::Config::from_file(config_path) {
        Ok(config) => config
            .services
            .iter()
            .filter_map(|(name, svc)| {
                svc.docker.as_ref().map(|d| {
                    d.container
                        .clone()
                        .unwrap_or_else(|| format!("don-{name}"))
                })
            })
            .collect(),
        Err(e) => {
            eprintln!("Warning: could not load config for docker cleanup: {e}");
            vec![]
        }
    };

    let report = don::process::cleanup::run_cleanup(&base, &docker_names).await;
    println!("{report}");

    // Hold lock until cleanup finishes, then release.
    drop(pid_lock);
    0
}

/// Read the PID from don.pid, send two SIGINTs (triggering the daemon's
/// own two-signal shutdown protocol: first = graceful, second = force SIGKILL
/// on all children), then wait for the process to exit.
async fn kill_running_daemon(pid_path: &std::path::Path) -> Result<(), String> {
    let content = std::fs::read_to_string(pid_path)
        .map_err(|e| format!("failed to read {}: {e}", pid_path.display()))?;
    let pid: i32 = content
        .trim()
        .parse()
        .map_err(|_| format!("invalid pid in {}: '{}'", pid_path.display(), content.trim()))?;

    let nix_pid = nix::unistd::Pid::from_raw(pid);

    // First SIGINT — triggers graceful shutdown.
    if nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGINT).is_err() {
        return Ok(()); // Already dead.
    }

    // Brief pause so the daemon registers the first signal and enters shutdown.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Second SIGINT — sets the force flag, daemon SIGKILLs all children.
    let _ = nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGINT);

    // Wait for the daemon to actually exit (up to 10s — it needs time to
    // reap children after SIGKILL).
    for _ in 0..100 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if nix::sys::signal::kill(nix_pid, None).is_err() {
            return Ok(()); // Process is gone.
        }
    }

    // Last resort — the daemon itself is stuck.
    eprintln!("daemon did not exit after 10s, sending SIGKILL");
    let _ = nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGKILL);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    Ok(())
}

fn validate(config_path: &std::path::Path) -> Result<(), String> {
    let config =
        don::config::Config::from_file(config_path).map_err(|e| format!("Error: {e}"))?;

    let platform = don::config::Platform::current().ok_or_else(|| {
        format!(
            "Unsupported platform: {}-{}",
            std::env::consts::OS,
            std::env::consts::ARCH
        )
    })?;

    let warnings = config
        .validate(platform)
        .map_err(|e| format!("Error: {e}"))?;
    for warning in &warnings {
        eprintln!("Warning: {warning}");
    }
    Ok(())
}

async fn run_start(config_path: &std::path::Path, profile: Option<&str>) -> Result<(), String> {
    let config =
        don::config::Config::from_file(config_path).map_err(|e| format!("Error: {e}"))?;

    let platform = don::config::Platform::current().ok_or_else(|| {
        format!(
            "Unsupported platform: {}-{}",
            std::env::consts::OS,
            std::env::consts::ARCH
        )
    })?;

    let warnings = config
        .validate(platform)
        .map_err(|e| format!("Error: {e}"))?;
    for warning in &warnings {
        eprintln!("Warning: {warning}");
    }

    let base = base_dir(config_path);

    // Collect service names and their log configs for OutputManager.
    let service_configs: Vec<(&str, &don::config::LogConfig)> = config
        .services
        .iter()
        .map(|(name, svc)| (name.as_str(), &svc.log))
        .collect();

    // Also include tasks in the output manager so they get prefixed output.
    let task_configs: Vec<(&str, &don::config::LogConfig)> = config
        .tasks
        .iter()
        .map(|(name, task)| (name.as_str(), &task.log))
        .collect();

    let all_configs: Vec<(&str, &don::config::LogConfig)> = service_configs
        .into_iter()
        .chain(task_configs)
        .collect();

    let output_manager = don::output::OutputManager::new(&all_configs, tokio::io::stdout())
        .await
        .map_err(|e| format!("Error creating output manager: {e}"))?;

    // Install signal handlers.
    let shutdown_rx = don::runner::install_signal_handlers()
        .await
        .map_err(|e| format!("Error installing signal handlers: {e}"))?;

    // Create and run the runner.
    let runner = don::runner::Runner::new(config, config_path.to_path_buf(), platform, output_manager, base, profile, shutdown_rx)
        .await
        .map_err(|e| format!("Error: {e}"))?;

    runner
        .run()
        .await
        .map_err(|e| format!("Error: {e}"))?;

    Ok(())
}
