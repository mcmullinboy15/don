// The CLI binary legitimately uses stdout — it IS the user-facing output.
#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]

use clap::{Parser, Subcommand};
use crossterm::style::{Attribute, Color, ResetColor, SetAttribute, SetForegroundColor};
use don::client::{Client, ClientError};
use don::runner::{ItemStatus, ServiceState, TaskItemState};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Write a line to stderr. Used for CLI error messages so they stay on the
/// error stream (scripts / tests / shell redirection expect it). We avoid
/// the `eprintln!` macro because it was frequently abused for debug-trace
/// output elsewhere in the codebase; keeping stderr writes behind this
/// single helper makes intentional error output easy to grep for.
fn errln(msg: impl std::fmt::Display) {
    let _ = writeln!(std::io::stderr(), "{msg}");
}

#[derive(Parser, Debug)]
#[command(
    name = "don",
    about = "Boss of your dev environment",
    arg_required_else_help = true
)]
struct Cli {
    /// Path to the config file
    #[arg(short, long, default_value = "don.toml", global = true)]
    config: PathBuf,

    /// Enable verbose output (timing info for lifecycle events)
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
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
    Status {
        /// Show detailed info: watch paths, ports, build tool targets, commands
        #[arg(short, long)]
        verbose: bool,
    },
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
    /// Run a task (bypasses auto_run)
    Run {
        /// Name of a specific task to run (mutually exclusive with --all-pending)
        name: Option<String>,
        /// Run all tasks currently in pending_run state
        #[arg(long, conflicts_with = "name")]
        all_pending: bool,
        /// Never prompt for missing required params — error instead. Implicit
        /// when stdin isn't a TTY. Useful in scripts / CI.
        #[arg(long, conflicts_with = "all_pending")]
        no_prompt: bool,
        /// Per-param flags. Parsed dynamically against the task's declared
        /// params: `--<param>=<value>`, `--<param> <value>`, or bare
        /// `--<flag>` (treated as `"true"` for bool params).
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        raw: Vec<String>,
    },
    /// Validate the config file
    Validate,
    /// Print shell completion script to stdout
    Completions {
        /// Target shell (bash, zsh, fish, powershell, elvish)
        shell: clap_complete::Shell,
    },
    /// Scaffold a starter don.toml in the current directory
    Init {
        /// Overwrite an existing don.toml
        #[arg(long)]
        force: bool,
    },
    /// Run a command with .don/bin on PATH (for downloaded binaries)
    Exec {
        /// Command to run
        cmd: String,
        /// Arguments passed to the command
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Internal: list names for shell completion scripts. Hidden from help.
    #[command(name = "__complete", hide = true)]
    Complete {
        /// One of: services, tasks, items, profiles
        kind: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let exit_code = run(cli.config, cli.verbose, cli.command).await;
    std::process::exit(exit_code);
}

async fn run(config_path: PathBuf, verbose: bool, command: Commands) -> i32 {
    match command {
        Commands::Validate => match validate(&config_path) {
            Ok(()) => {
                println!("Config is valid.");
                0
            }
            Err(e) => {
                errln(e);
                1
            }
        },
        Commands::Start {
            profile,
            name: None,
        } => match run_start(&config_path, profile.as_deref(), verbose).await {
            Ok(()) => 0,
            Err(e) => {
                errln(e);
                1
            }
        },
        Commands::Start {
            name: Some(name), ..
        } => run_client(&config_path, |c| async move { c.start(&name).await }).await,
        Commands::Stop { name } => {
            run_client(&config_path, |c| async move { c.stop(&name).await }).await
        }
        Commands::Restart { name } => {
            run_client(&config_path, |c| async move { c.restart(&name).await }).await
        }
        Commands::Status { verbose } => run_status(&config_path, verbose).await,
        Commands::Logs { name, last, follow } => run_logs(&config_path, &name, last, follow).await,
        Commands::Attach { name } => run_attach(&config_path, &name).await,
        Commands::Cleanup { force } => run_cleanup_command(&config_path, force).await,
        Commands::Run {
            name,
            all_pending,
            raw,
            no_prompt,
        } => match (name, all_pending) {
            (Some(n), _) => run_run_task(&config_path, n, raw, no_prompt).await,
            (None, true) => {
                run_client(&config_path, |c| async move { c.run_pending().await }).await
            }
            (None, false) => {
                errln("don run: provide a task name or --all-pending");
                1
            }
        },
        Commands::Completions { shell } => {
            let mut out = std::io::stdout();
            match don::completions::emit_script::<_, Cli>(shell, "don", &mut out) {
                Ok(()) => 0,
                Err(e) => {
                    errln(format!("failed to write completion script: {e}"));
                    1
                }
            }
        }
        Commands::Complete { kind } => {
            // Silent-on-error: invoked inside tab-completion. If the kind is
            // unknown or the config is broken, print nothing and exit 0 so
            // the user's shell doesn't spew errors mid-tab-press.
            if let Ok(kind) = kind.parse::<don::completions::CompleteKind>() {
                for name in don::completions::list_names(kind, &config_path) {
                    println!("{name}");
                }
            }
            0
        }
        Commands::Init { force } => match don::init::write_starter_config(&config_path, force) {
            Ok(()) => {
                println!("created {}", config_path.display());
                0
            }
            Err(e) => {
                errln(e);
                1
            }
        },
        Commands::Exec { cmd, args } => {
            let base = base_dir(&config_path);
            match don::exec::exec_with_don_path(&base, &cmd, &args) {
                Ok(()) => 0, // unreachable — execvp returns only on error
                Err(e) => {
                    errln(format!("don exec {cmd}: {e}"));
                    127
                }
            }
        }
    }
}

fn client_for(config_path: &Path) -> Client {
    Client::new(base_dir(config_path).as_path())
}

/// Handle `don run <task> [flags]`. Parses `raw` against the task's declared
/// params and dispatches via the client.
async fn run_run_task(config_path: &Path, name: String, raw: Vec<String>, no_prompt: bool) -> i32 {
    // Load config to look up the task's params. This duplicates what the
    // runner does server-side, but we need the param list *here* to
    // parse the trailing args correctly.
    let config = match don::config::Config::from_file(config_path) {
        Ok(c) => c,
        Err(e) => {
            errln(format!("failed to load config: {e}"));
            return 1;
        }
    };
    let Some(task) = config.tasks.get(&name) else {
        errln(format!("unknown task '{name}'"));
        return 1;
    };

    let parsed = match parse_task_args(&raw, &task.params) {
        Ok(p) => p,
        Err(msg) => {
            errln(msg);
            return 2;
        }
    };

    // Interactive TTY mode could open a form here for missing required
    // params. Until the TUI form is wired into `don run` itself, we keep
    // the CLI in strict mode: error out when required params are missing
    // and the user hasn't supplied `--no-prompt` (it's implied).
    let _ = no_prompt;
    for p in &task.params {
        if p.required && !parsed.contains_key(&p.name) && p.default.is_none() {
            errln(format!(
                "missing required param --{} (run `don start` and use the palette form, \
                 or pass --{}=<value>)",
                p.name, p.name
            ));
            return 2;
        }
    }

    let client = client_for(config_path);
    match client.run_task(&name, parsed).await {
        Ok(()) => 0,
        Err(e) => {
            errln(e);
            1
        }
    }
}

/// Parse the trailing raw args from `don run <task> …` against the task's
/// declared params. Accepts:
/// - `--<name>=<value>`
/// - `--<name> <value>`
/// - bare `--<flag>` (kind = Bool → "true")
///
/// Returns a map of user-supplied values, or a user-facing error string.
fn parse_task_args(
    raw: &[String],
    params: &[don::config::TaskParam],
) -> Result<std::collections::HashMap<String, String>, String> {
    use don::config::ParamKind;

    let by_name: std::collections::HashMap<&str, &don::config::TaskParam> =
        params.iter().map(|p| (p.name.as_str(), p)).collect();
    let known_names: Vec<&str> = params.iter().map(|p| p.name.as_str()).collect();

    let mut out = std::collections::HashMap::new();
    let mut i = 0;
    while i < raw.len() {
        let arg = &raw[i];
        if !arg.starts_with("--") {
            return Err(format!(
                "unexpected positional arg '{arg}' — don run expects --<name>=<value> flags"
            ));
        }
        let stripped = &arg[2..];
        let (name, value) = match stripped.split_once('=') {
            Some((k, v)) => (k.to_string(), Some(v.to_string())),
            None => (stripped.to_string(), None),
        };
        let Some(param) = by_name.get(name.as_str()) else {
            let valid = if known_names.is_empty() {
                "task declares no params".to_string()
            } else {
                format!("valid: --{}", known_names.join(", --"))
            };
            return Err(format!("unknown param '--{name}' ({valid})"));
        };
        let resolved = match value {
            Some(v) => v,
            None => match param.kind {
                ParamKind::Bool => {
                    // Bare `--flag` = true for bools.
                    i += 1;
                    out.insert(name, "true".to_string());
                    continue;
                }
                _ => {
                    // Expect the next token to be the value.
                    match raw.get(i + 1) {
                        Some(v) if !v.starts_with("--") => {
                            let v = v.clone();
                            i += 2;
                            out.insert(name, v);
                            continue;
                        }
                        _ => {
                            return Err(format!(
                                "param --{name} is missing a value (use --{name}=<value>)"
                            ));
                        }
                    }
                }
            },
        };
        i += 1;
        out.insert(name, resolved);
    }
    Ok(out)
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
            errln(e);
            1
        }
    }
}

async fn run_status(config_path: &Path, verbose: bool) -> i32 {
    let client = client_for(config_path);
    match client.status(verbose).await {
        Ok(mut items) => {
            items.sort_by(|a, b| {
                status_sort_bucket(a)
                    .cmp(&status_sort_bucket(b))
                    .then_with(|| item_name(a).cmp(item_name(b)))
            });
            print_status_table(&items, verbose);
            0
        }
        Err(e) => {
            errln(e);
            1
        }
    }
}

/// Sort bucket for the status table: genuine failures first, then the
/// dependency-failed cascade, then running, then exited, then lazy. Putting
/// `DependencyFailed` *below* `Failed` surfaces the actual culprit — the
/// thing the user needs to look at — above everything that merely got
/// stranded.
fn status_sort_bucket(item: &ItemStatus) -> u8 {
    match item {
        ItemStatus::Service { state, .. } => match state {
            ServiceState::Failed | ServiceState::Unhealthy => 0,
            ServiceState::DependencyFailed => 1,
            ServiceState::Pending
            | ServiceState::Building
            | ServiceState::Starting
            | ServiceState::Running
            | ServiceState::Ready
            | ServiceState::Stopping => 2,
            ServiceState::Stopped => 3,
            ServiceState::Lazy => 4,
        },
        ItemStatus::Task { state, .. } => match state {
            TaskItemState::Failed => 0,
            TaskItemState::DependencyFailed => 1,
            TaskItemState::Pending
            | TaskItemState::Building
            | TaskItemState::Running
            | TaskItemState::Completed
            | TaskItemState::Skipped
            | TaskItemState::PendingRun => 2,
        },
    }
}

fn item_name(item: &ItemStatus) -> &str {
    match item {
        ItemStatus::Service { name, .. } | ItemStatus::Task { name, .. } => name.as_str(),
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
                errln(e);
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
                errln(e);
                1
            }
        }
    }
}

async fn run_attach(config_path: &Path, name: &str) -> i32 {
    let base = base_dir(config_path);
    let socket_path = base.join(".don").join("don.sock");
    match don::client::attach::run_attach(&socket_path, name).await {
        Ok(()) => {
            println!("\r\ndetached from '{name}'");
            0
        }
        Err(e) => {
            errln(e);
            1
        }
    }
}

fn print_status_table(items: &[ItemStatus], verbose: bool) {
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
        let (kind, name, state_str, color, verbose_info) = match item {
            ItemStatus::Service {
                name,
                state,
                verbose,
            } => (
                "service",
                name.as_str(),
                service_state_label(*state),
                service_state_color(*state),
                verbose.as_ref(),
            ),
            ItemStatus::Task {
                name,
                state,
                verbose,
            } => (
                "task",
                name.as_str(),
                task_state_label(*state),
                task_state_color(*state),
                verbose.as_ref(),
            ),
        };
        println!(
            "{:<kind_w$}  {:<name_w$}  {}{}{}",
            kind,
            name,
            SetForegroundColor(color),
            state_str,
            ResetColor,
        );

        if verbose && let Some(info) = verbose_info {
            print_verbose_info(info);
        }
    }
}

/// Print verbose details for a single item, indented under the status line.
#[allow(clippy::print_stdout)]
fn print_verbose_info(info: &don::runner::VerboseInfo) {
    let dim = SetAttribute(Attribute::Dim);
    let reset = SetAttribute(Attribute::Reset);

    if let Some(ref cmd) = info.cmd {
        println!("  {dim}cmd:{reset}    {cmd}");
    }
    if !info.depends_on.is_empty() {
        println!("  {dim}deps:{reset}   {}", info.depends_on.join(", "));
    }
    if !info.proxy.is_empty() {
        println!("  {dim}proxy:{reset}  {}", info.proxy.join(", "));
    }
    if let Some(ref ready) = info.ready {
        println!("  {dim}ready:{reset}  {ready}");
    }
    if let Some(ref target) = info.bazel_target {
        println!("  {dim}bazel:{reset}  {target}");
    }
    if let Some(ref task) = info.turbo_task {
        println!("  {dim}turbo:{reset}  {task}");
    }
    if !info.watch.is_empty() {
        println!(
            "  {dim}watch:{reset}  {}",
            info.watch.first().unwrap_or(&String::new())
        );
        for pattern in info.watch.iter().skip(1) {
            println!("         {pattern}");
        }
    }
    if let Some(ref watch_state) = info.watch_state {
        println!("  {dim}watch state:{reset}  {watch_state}");
    }
    if !info.watch_notes.is_empty() {
        println!("  {dim}watch diag:{reset}   {}", info.watch_notes[0]);
        for note in info.watch_notes.iter().skip(1) {
            println!("               {note}");
        }
    }
}

fn service_state_label(s: ServiceState) -> &'static str {
    match s {
        ServiceState::Pending => "pending",
        ServiceState::Building => "building",
        ServiceState::Lazy => "lazy",
        ServiceState::Starting => "starting",
        ServiceState::Running => "running",
        ServiceState::Ready => "ready",
        ServiceState::Unhealthy => "unhealthy",
        ServiceState::Stopping => "stopping",
        ServiceState::Stopped => "stopped",
        ServiceState::Failed => "failed",
        ServiceState::DependencyFailed => "dep failed",
    }
}

fn service_state_color(s: ServiceState) -> Color {
    match s {
        ServiceState::Ready | ServiceState::Running => Color::Green,
        ServiceState::Starting
        | ServiceState::Building
        | ServiceState::Pending
        | ServiceState::Stopping => Color::Yellow,
        ServiceState::Lazy => Color::Cyan,
        ServiceState::Stopped => Color::DarkGrey,
        ServiceState::Unhealthy => Color::Red,
        ServiceState::Failed => Color::Red,
        // Dim red: same hue family as Failed so the user sees it's in the
        // error-neighbourhood, but visually quieter than the culprit above it.
        ServiceState::DependencyFailed => Color::DarkRed,
    }
}

fn task_state_label(s: TaskItemState) -> &'static str {
    match s {
        TaskItemState::Pending => "pending",
        TaskItemState::Building => "building",
        TaskItemState::Running => "running",
        TaskItemState::Completed => "completed",
        TaskItemState::Skipped => "skipped",
        TaskItemState::Failed => "failed",
        TaskItemState::DependencyFailed => "dep failed",
        TaskItemState::PendingRun => "pending_run",
    }
}

fn task_state_color(s: TaskItemState) -> Color {
    match s {
        TaskItemState::Completed | TaskItemState::Skipped => Color::Green,
        TaskItemState::Running | TaskItemState::Pending | TaskItemState::Building => Color::Yellow,
        TaskItemState::PendingRun => Color::Cyan,
        TaskItemState::Failed => Color::Red,
        TaskItemState::DependencyFailed => Color::DarkRed,
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
            errln("killing running don daemon...");
            if let Err(e) = kill_running_daemon(&don_pid_path).await {
                errln(format!("failed to kill daemon: {e}"));
                return 1;
            }
            // Now re-acquire the lock.
            match don::process::pid_file::PidFile::acquire(don_pid_path, std::process::id() as i32)
                .await
            {
                Ok(lock) => lock,
                Err(e) => {
                    errln(format!("failed to acquire pid lock after kill: {e}"));
                    return 1;
                }
            }
        }
        Err(e) => {
            errln(format!("failed to acquire pid lock: {e}"));
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
                if let Some(don::config::ServiceKind::Docker(d)) = &svc.kind {
                    Some(d.container.clone().unwrap_or_else(|| format!("don-{name}")))
                } else {
                    None
                }
            })
            .collect(),
        Err(e) => {
            errln(format!(
                "Warning: could not load config for docker cleanup: {e}"
            ));
            vec![]
        }
    };

    let report = don::process::cleanup::run_cleanup(&base, &docker_names).await;
    println!("{report}");
    for warning in &report.warnings {
        errln(format!("Warning: {warning}"));
    }

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
    let pid: i32 = content.trim().parse().map_err(|_| {
        format!(
            "invalid pid in {}: '{}'",
            pid_path.display(),
            content.trim()
        )
    })?;

    let nix_pid = nix::unistd::Pid::from_raw(pid);

    // First SIGINT — triggers graceful shutdown.
    if nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGINT).is_err() {
        return Ok(()); // Already dead.
    }

    // Brief pause so the daemon registers the first signal and enters shutdown.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Second SIGINT — sets the force flag, daemon SIGKILLs all children.
    let _ = nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGINT);

    // Wait for the daemon to actually exit (up to 1s — it needs time to
    // reap children after SIGKILL).
    for _ in 0..10 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if nix::sys::signal::kill(nix_pid, None).is_err() {
            return Ok(()); // Process is gone.
        }
    }

    // Last resort — the daemon itself is stuck.
    errln("daemon did not exit after 1s, sending SIGKILL");
    let _ = nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGKILL);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    Ok(())
}

fn validate(config_path: &std::path::Path) -> Result<(), String> {
    let config = don::config::Config::from_file(config_path).map_err(|e| format!("Error: {e}"))?;

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
        errln(format!("Warning: {warning}"));
    }
    Ok(())
}

async fn run_start(
    config_path: &std::path::Path,
    profile: Option<&str>,
    verbose: bool,
) -> Result<(), String> {
    use std::io::IsTerminal;

    let config = don::config::Config::from_file(config_path).map_err(|e| format!("Error: {e}"))?;

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
        errln(format!("Warning: {warning}"));
    }

    let base = base_dir(config_path);
    let is_tty = std::io::stdout().is_terminal();

    // Fall back to the config's default_profile when `--profile` is not given.
    // Validation above guarantees default_profile (if set) is a known profile,
    // so the lookup below cannot miss. Own the string so later code can move
    // `config` into `Runner::new` while we still hold the profile name.
    let profile: Option<String> = profile
        .map(str::to_string)
        .or_else(|| config.default_profile.clone());
    let profile_ref: Option<&str> = profile.as_deref();

    // Resolve the active item set up front so the output manager and TUI
    // only see items that will actually run. Without this, prefix padding
    // is sized for the longest name in the whole config, and the TUI
    // service menu lists items the profile excludes. The runner re-runs
    // this inside `Runner::new` to build its own filtered state.
    let active_items: Option<std::collections::HashSet<String>> =
        if let Some(profile_name) = profile_ref {
            let prof = config
                .profiles
                .get(profile_name)
                .ok_or_else(|| format!("Error: unknown profile '{profile_name}'"))?;
            Some(don::runner::resolve_profile_items(&config, prof))
        } else {
            None
        };

    let is_active = |name: &str| active_items.as_ref().is_none_or(|s| s.contains(name));

    // Collect service names and their log configs for OutputManager.
    let service_configs: Vec<(&str, &don::config::LogConfig)> = config
        .services
        .iter()
        .filter(|(name, _)| is_active(name))
        .map(|(name, svc)| (name.as_str(), &svc.log))
        .collect();

    // Also include tasks in the output manager so they get prefixed output.
    let task_configs: Vec<(&str, &don::config::LogConfig)> = config
        .tasks
        .iter()
        .filter(|(name, _)| is_active(name))
        .map(|(name, task)| (name.as_str(), &task.log))
        .collect();

    // Synthetic build-tool stream names should participate in the initial
    // output palette so color choice and prefix width depend only on the
    // config-derived item set, not on later registration order.
    let service_kinds = || {
        config.services.values().flat_map(|svc| {
            std::iter::once(svc.kind.as_ref())
                .chain(svc.platform.values().map(|ov| ov.kind.as_ref()))
                .flatten()
        })
    };
    let uses_bazel = service_kinds().any(|k| matches!(k, don::config::ServiceKind::Bazel(_)))
        || config.tasks.values().any(|t| t.bazel.is_some());
    let uses_turbo = service_kinds().any(|k| matches!(k, don::config::ServiceKind::Turbo(_)))
        || config.tasks.values().any(|t| t.turbo.is_some());
    let build_tool_log = don::config::LogConfig::Stdout;
    let build_tool_configs: Vec<(&str, &don::config::LogConfig)> = [
        uses_bazel.then_some(("bazel", &build_tool_log)),
        uses_turbo.then_some(("turbo", &build_tool_log)),
    ]
    .into_iter()
    .flatten()
    .collect();

    let all_configs: Vec<(&str, &don::config::LogConfig)> = service_configs
        .into_iter()
        .chain(task_configs)
        .chain(build_tool_configs.iter().copied())
        .collect();

    // Install signal handlers before building the runner so Ctrl+C still
    // reaches the graceful-shutdown path even during a slow startup.
    let shutdown_rx = don::runner::install_signal_handlers()
        .await
        .map_err(|e| format!("Error installing signal handlers: {e}"))?;

    if is_tty {
        let (output_manager, log_rx) =
            don::output::OutputManager::new_with_tui(&all_configs, verbose)
                .await
                .map_err(|e| format!("Error creating output manager: {e}"))?;
        let verbosity = output_manager.verbosity_control();
        let lifecycle_emitter = output_manager.clone_lifecycle_emitter();

        let service_names: Vec<String> = config
            .services
            .keys()
            .filter(|name| is_active(name))
            .cloned()
            .collect();
        let task_names: Vec<String> = config
            .tasks
            .keys()
            .filter(|name| is_active(name))
            .cloned()
            .collect();

        // Snapshot the task configs before moving `config` into the runner —
        // the TUI form needs the param schema to render prompts and to route
        // per-param completion requests.
        let task_configs: std::collections::HashMap<String, don::config::Task> =
            config.tasks.clone();

        // Synthetic build-tool stream names that should appear in the TUI
        // filter. Without these entries, lines emitted by the bazel/turbo
        // clients (which carry `name = "bazel"` / `"turbo"`) are silently
        // dropped by the filter's allowlist — the user sees nothing during
        // the build phase.
        let build_tool_names: Vec<String> = build_tool_configs
            .iter()
            .map(|(name, _)| (*name).to_string())
            .collect();

        // Collect names whose `hidden = true` flag should start them outside
        // the TUI filter's default selection. Both services and tasks can
        // opt in — the filter treats them identically.
        let hidden_names: std::collections::HashSet<String> = config
            .services
            .iter()
            .filter(|(name, svc)| is_active(name) && svc.hidden)
            .map(|(name, _)| name.clone())
            .chain(
                config
                    .tasks
                    .iter()
                    .filter(|(name, task)| is_active(name) && task.hidden)
                    .map(|(name, _)| name.clone()),
            )
            .collect();

        let runner = await_with_shutdown_supervision(
            tokio::spawn({
                let profile = profile.clone();
                async move {
                    don::runner::Runner::new(
                        config,
                        platform,
                        output_manager,
                        base,
                        profile.as_deref(),
                        shutdown_rx,
                    )
                    .await
                    .map_err(|e| format!("Error: {e}"))
                }
            }),
            "starting runner",
        )
        .await?;

        let events = runner.subscribe();
        let commands = runner.command_sender();

        // Wrap the TUI so that if it exits unexpectedly (e.g. a terminal IO
        // error or panic), we signal the runner to shut down instead of
        // leaving the daemon alive while the user's terminal is in cooked
        // mode. Without this, raw mode gets disabled but logs keep streaming —
        // the user sees a free-floating cursor and can type into the shell
        // while the runner runs unattended. The log_rx closing (runner
        // shutdown) returns Ok(()) so the normal exit path is unaffected.
        let tui = tokio::spawn(async move {
            let result = don::tui::run_tui(
                log_rx,
                events,
                commands,
                verbosity,
                lifecycle_emitter,
                service_names,
                task_names,
                build_tool_names,
                task_configs,
                hidden_names,
            )
            .await;
            if result.is_err() {
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::this(),
                    nix::sys::signal::Signal::SIGINT,
                );
            }
            result
        });

        let runner_task =
            tokio::spawn(async move { runner.run().await.map_err(|e| format!("Error: {e}")) });
        let runner_result =
            await_with_shutdown_supervision(runner_task, "waiting for runner shutdown").await;
        if runner_result.is_err() {
            tui.abort();
        }

        // Surface any TUI error so unexpected exits are visible instead of
        // silently dropped. Runner errors take precedence.
        match tui.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => errln(format!("TUI error: {e}")),
            Err(join_err) if join_err.is_panic() => {
                errln(format!("TUI task panicked: {join_err}"));
            }
            Err(_) => {} // cancelled — expected on shutdown
        }

        runner_result
    } else {
        let output_manager =
            don::output::OutputManager::new_verbose(&all_configs, tokio::io::stdout(), verbose)
                .await
                .map_err(|e| format!("Error creating output manager: {e}"))?;

        let runner = await_with_shutdown_supervision(
            tokio::spawn({
                let profile = profile.clone();
                async move {
                    don::runner::Runner::new(
                        config,
                        platform,
                        output_manager,
                        base,
                        profile.as_deref(),
                        shutdown_rx,
                    )
                    .await
                    .map_err(|e| format!("Error: {e}"))
                }
            }),
            "starting runner",
        )
        .await?;

        let runner_task =
            tokio::spawn(async move { runner.run().await.map_err(|e| format!("Error: {e}")) });
        await_with_shutdown_supervision(runner_task, "waiting for runner shutdown").await
    }
}

async fn await_with_shutdown_supervision<T>(
    mut handle: tokio::task::JoinHandle<Result<T, String>>,
    phase: &str,
) -> Result<T, String>
where
    T: Send + 'static,
{
    // Process-level shutdown supervision lives outside the runner. The runner
    // gets the first chance to unwind cleanly. `main` only force-aborts the
    // runner task if a second Ctrl+C arrives, mirroring the daemon's own
    // two-signal shutdown semantics.
    let poll_interval = std::time::Duration::from_millis(100);

    loop {
        if don::runner::signal_count() >= 2 {
            errln(format!("forcing exit while {phase}"));
            handle.abort();
            let _ = handle.await;
            return Err(format!("forced exit while {phase}"));
        }

        tokio::select! {
            result = &mut handle => return map_join_result(result, phase),
            _ = tokio::time::sleep(poll_interval) => {}
        }
    }
}

fn map_join_result<T>(
    result: Result<Result<T, String>, tokio::task::JoinError>,
    phase: &str,
) -> Result<T, String> {
    match result {
        Ok(inner) => inner,
        Err(join_err) if join_err.is_cancelled() => Err(format!("cancelled while {phase}")),
        Err(join_err) => Err(format!("task failed while {phase}: {join_err}")),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::parse_task_args;
    use don::config::{ParamKind, TaskParam};

    fn p(name: &str) -> TaskParam {
        TaskParam {
            name: name.to_string(),
            prompt: None,
            required: false,
            default: None,
            kind: ParamKind::String,
            choices: vec![],
            completions: None,
            validate: None,
        }
    }

    #[test]
    fn parse_table() {
        struct Case {
            name: &'static str,
            params: Vec<TaskParam>,
            raw: Vec<&'static str>,
            want_ok: Option<Vec<(&'static str, &'static str)>>,
            want_err: Option<&'static str>,
        }

        let cases = vec![
            Case {
                name: "empty args",
                params: vec![],
                raw: vec![],
                want_ok: Some(vec![]),
                want_err: None,
            },
            Case {
                name: "key=value",
                params: vec![p("index")],
                raw: vec!["--index=users"],
                want_ok: Some(vec![("index", "users")]),
                want_err: None,
            },
            Case {
                name: "separated",
                params: vec![p("index")],
                raw: vec!["--index", "users"],
                want_ok: Some(vec![("index", "users")]),
                want_err: None,
            },
            Case {
                name: "mixed",
                params: vec![p("a"), p("b")],
                raw: vec!["--a=1", "--b", "two"],
                want_ok: Some(vec![("a", "1"), ("b", "two")]),
                want_err: None,
            },
            Case {
                name: "bool bare flag",
                params: vec![TaskParam {
                    kind: ParamKind::Bool,
                    ..p("enabled")
                }],
                raw: vec!["--enabled"],
                want_ok: Some(vec![("enabled", "true")]),
                want_err: None,
            },
            Case {
                name: "bool explicit value",
                params: vec![TaskParam {
                    kind: ParamKind::Bool,
                    ..p("enabled")
                }],
                raw: vec!["--enabled=false"],
                want_ok: Some(vec![("enabled", "false")]),
                want_err: None,
            },
            Case {
                name: "unknown flag",
                params: vec![p("a")],
                raw: vec!["--b=1"],
                want_ok: None,
                want_err: Some("unknown param '--b'"),
            },
            Case {
                name: "missing value",
                params: vec![p("a")],
                raw: vec!["--a"],
                want_ok: None,
                want_err: Some("missing a value"),
            },
            Case {
                name: "positional rejected",
                params: vec![p("a")],
                raw: vec!["stray"],
                want_ok: None,
                want_err: Some("unexpected positional"),
            },
            Case {
                name: "value that looks like a flag consumed by separated form errors",
                params: vec![p("a")],
                raw: vec!["--a", "--b"],
                want_ok: None,
                want_err: Some("missing a value"),
            },
            Case {
                name: "value starting with dash via equals form is fine",
                params: vec![p("a")],
                raw: vec!["--a=-x"],
                want_ok: Some(vec![("a", "-x")]),
                want_err: None,
            },
        ];

        for case in cases {
            let raw: Vec<String> = case.raw.iter().map(|s| s.to_string()).collect();
            let got = parse_task_args(&raw, &case.params);
            match (got, case.want_ok, case.want_err) {
                (Ok(m), Some(want), None) => {
                    let want_map: std::collections::HashMap<String, String> = want
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect();
                    assert_eq!(m, want_map, "{}", case.name);
                }
                (Err(e), None, Some(needle)) => {
                    assert!(
                        e.contains(needle),
                        "{}: err '{e}' missing '{needle}'",
                        case.name
                    );
                }
                (got, ok, err) => panic!(
                    "{}: got {:?}, want ok={:?} err={:?}",
                    case.name, got, ok, err
                ),
            }
        }
    }
}
