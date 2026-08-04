//! The system-wide don daemon.
//!
//! Don's per-project model is unchanged by this module: `don start` still
//! owns its own process group, its own `.don/` directory, and its own
//! unix-socket API. The daemon is a *broker* — it holds a registry of the
//! projects that are currently running and serves the web UI on top of them,
//! reverse-proxying each project's existing API. It never spawns or
//! supervises a service itself.
//!
//! That split is what keeps registration cheap and optional: a project that
//! can't reach the daemon just doesn't appear in the UI, and nothing about
//! the stack it's running changes. It also means stopping the daemon is
//! harmless — every registered project keeps running, it just loses its
//! window into the browser.

pub mod client;
pub mod paths;
pub mod registry;
pub(crate) mod routes;

pub use client::DaemonClient;
pub use paths::{DaemonEnv, DaemonPaths, PathError};
pub use registry::{ProjectEntry, ProjectRegistry};

use crate::process::pid_file::{PidFile, PidFileError};
use routes::{ControlState, DaemonCommand};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Errors that stop the daemon from starting or running.
#[derive(Debug, thiserror::Error)]
pub enum DaemonError {
    /// State directory resolution or creation failed.
    #[error(transparent)]
    Paths(#[from] PathError),
    /// Another daemon already holds the PID file.
    #[error(
        "a don daemon is already running (pid file: {path}) — \
         use `don daemon status` to inspect it or `don daemon stop` to stop it"
    )]
    AlreadyRunning { path: String },
    /// Acquiring the PID file failed for some other reason.
    #[error("failed to lock the daemon pid file: {0}")]
    PidFile(#[source] PidFileError),
    /// Binding the control socket failed.
    #[error(transparent)]
    Server(#[from] crate::server::ServerError),
    /// Installing signal handlers failed.
    #[error("failed to install signal handlers: {0}")]
    Signals(#[source] std::io::Error),
}

/// How the daemon should run.
#[derive(Debug, Clone)]
pub struct DaemonOptions {
    /// Where the daemon keeps its socket, registry, and token.
    pub paths: DaemonPaths,
    /// Address to serve the web UI on. `None` runs the control plane only,
    /// which is useful for tests and for diagnosing registration on its own.
    pub web_addr: Option<SocketAddr>,
}

/// A line the daemon wants to report to whoever is watching it.
///
/// The daemon writes to plain stdout under systemd/launchd, but tests and
/// `don daemon` in the foreground want the same stream, so events go through
/// a callback rather than `println!` (which the crate lints against anyway).
pub type Reporter = Arc<dyn Fn(&str) + Send + Sync>;

/// Run the daemon until it is asked to stop.
///
/// Stops on SIGINT/SIGTERM or a `POST /shutdown` on the control socket.
/// Registered projects are deliberately left alone on the way out.
pub async fn run(options: DaemonOptions, report: Reporter) -> Result<(), DaemonError> {
    options.paths.ensure()?;

    // Single-instance guard. Taken before binding anything so a second
    // daemon fails with a clear message instead of stealing the socket.
    let pid_path = options.paths.pid_file();
    let _pid_lock = PidFile::acquire(pid_path.clone(), std::process::id() as i32)
        .await
        .map_err(|e| match e {
            PidFileError::AlreadyLocked => DaemonError::AlreadyRunning {
                path: pid_path.display().to_string(),
            },
            other => DaemonError::PidFile(other),
        })?;

    let (registry, outcome) = ProjectRegistry::load(options.paths.registry());
    match &outcome {
        registry::LoadOutcome::Fresh => {}
        registry::LoadOutcome::Restored { count } => {
            report(&format!("restored {count} project(s) from the registry"));
        }
        registry::LoadOutcome::Discarded { reason } => {
            report(&format!(
                "registry could not be read ({reason}) — starting empty; \
                 running projects will re-register on their next start"
            ));
        }
    }

    let socket_path = options.paths.socket();
    let listener = crate::server::bind_api(&socket_path)?;

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let web_addr = options.web_addr.map(|a| a.to_string());
    let control = Arc::new(ControlState {
        cmd_tx: cmd_tx.clone(),
        web_addr: web_addr.clone(),
    });
    let router = routes::build_router(control);

    let server = tokio::spawn(crate::server::serve_router(
        listener,
        socket_path.clone(),
        router,
        shutdown_rx,
    ));

    report(&format!("control socket listening on {}", socket_path.display()));
    match &web_addr {
        Some(addr) => report(&format!("web ui on http://{addr}")),
        None => report("web ui disabled"),
    }

    let signals = crate::runner::install_signal_handlers()
        .await
        .map_err(DaemonError::Signals)?;

    // The registry task owns the map outright; every reader and writer goes
    // through `cmd_tx`. Same shape as the runner's command loop, and for the
    // same reason — one owner means no locks and no lock-ordering to get wrong.
    registry_loop(registry, cmd_rx, signals, &report).await;

    let _ = shutdown_tx.send(true);
    let _ = server.await;
    report("daemon stopped");
    Ok(())
}

/// Own the registry and serve commands until shutdown.
async fn registry_loop(
    mut registry: ProjectRegistry,
    mut cmd_rx: mpsc::UnboundedReceiver<DaemonCommand>,
    mut signals: mpsc::Receiver<()>,
    report: &Reporter,
) {
    // Anything that died while the daemon was down is already stale.
    let removed = registry.prune().await;
    report_pruned(&removed, report);
    persist(&registry, report);

    loop {
        tokio::select! {
            _ = signals.recv() => {
                report("signal received, stopping");
                break;
            }
            command = cmd_rx.recv() => {
                let Some(command) = command else { break };
                match command {
                    DaemonCommand::Shutdown => {
                        report("shutdown requested");
                        break;
                    }
                    DaemonCommand::Register { entry, reply } => {
                        let name = entry.name.clone();
                        let replaced = registry.register(*entry);
                        report(&format!(
                            "{} project '{name}'",
                            if replaced { "re-registered" } else { "registered" }
                        ));
                        persist(&registry, report);
                        let _ = reply.send(());
                    }
                    DaemonCommand::Deregister { id, reply } => {
                        let removed = registry.deregister(&id);
                        if removed {
                            report("deregistered a project");
                            persist(&registry, report);
                        }
                        let _ = reply.send(removed);
                    }
                    DaemonCommand::List { reply } => {
                        let removed = registry.prune().await;
                        if !removed.is_empty() {
                            report_pruned(&removed, report);
                            persist(&registry, report);
                        }
                        let _ = reply.send(registry.list());
                    }
                    DaemonCommand::Count { reply } => {
                        let _ = reply.send(registry.len());
                    }
                }
            }
        }
    }

    persist(&registry, report);
}

fn report_pruned(removed: &[ProjectEntry], report: &Reporter) {
    for entry in removed {
        report(&format!(
            "dropped '{}' — its socket at {} no longer answers",
            entry.name,
            entry.socket.display()
        ));
    }
}

/// Persisting is a cache update, never a reason to fail a request — a
/// read-only state directory should cost the user their registry surviving a
/// daemon restart, nothing more.
fn persist(registry: &ProjectRegistry, report: &Reporter) {
    if let Err(e) = registry.persist() {
        report(&format!("could not save the project registry: {e}"));
    }
}
