use clap::{Parser, Subcommand};
use std::path::PathBuf;

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
        #[arg(short, long)]
        last: Option<usize>,
    },
    /// Interactively attach stdin/stdout to a running service
    Attach {
        /// Name of the service to attach to
        name: String,
    },
    /// Clean up stale state from a previous run
    Cleanup,
    /// Validate the config file
    Validate,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let command = cli.command.unwrap_or(Commands::Start { profile: None });

    match command {
        Commands::Validate => {
            if let Err(e) = validate(&cli.config) {
                eprintln!("{e}");
                std::process::exit(1);
            }
            println!("Config is valid.");
        }
        Commands::Start { .. } => {
            if let Err(e) = run_start(&cli.config).await {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }
        Commands::Stop { name } => {
            eprintln!("don stop {name}: not yet implemented");
        }
        Commands::Restart { name } => {
            eprintln!("don restart {name}: not yet implemented");
        }
        Commands::Status => {
            eprintln!("don status: not yet implemented");
        }
        Commands::Logs { name, .. } => {
            eprintln!("don logs {name}: not yet implemented");
        }
        Commands::Attach { name } => {
            eprintln!("don attach {name}: not yet implemented");
        }
        Commands::Cleanup => {
            eprintln!("don cleanup: not yet implemented");
        }
    }
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

async fn run_start(config_path: &std::path::Path) -> Result<(), String> {
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

    // Determine base directory (where don.toml lives).
    // `parent()` returns `Some("")` for a bare filename like `don.toml` —
    // treat that as the current directory.
    let base_dir = match config_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => std::path::PathBuf::from("."),
    };

    // Collect service names and their log configs for OutputManager.
    let service_configs: Vec<(&str, &don::config::LogConfig)> = config
        .services
        .iter()
        .map(|(name, svc)| {
            // We need the resolved log config, but OutputManager takes references.
            // For now, use the base service's log config.
            (name.as_str(), &svc.log)
        })
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
    let runner = don::runner::Runner::new(config, platform, output_manager, base_dir, shutdown_rx)
        .await
        .map_err(|e| format!("Error: {e}"))?;

    runner
        .run()
        .await
        .map_err(|e| format!("Error: {e}"))?;

    Ok(())
}
