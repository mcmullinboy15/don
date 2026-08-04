//! Unix-socket HTTP API — CLI ↔ daemon communication.
//!
//! Serves an axum router over `.don/don.sock` so CLI subcommands like
//! `don status`, `don stop`, `don logs` can talk to the running daemon.
//! The runner binds the socket synchronously (so bind errors surface
//! immediately) and then spawns the accept loop as a background task.

pub(crate) mod attach;
pub(crate) mod routes;

use crate::runner::{RunnerCommand, RunnerEvent};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio::sync::{broadcast, mpsc, watch};

/// Server errors.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("failed to bind unix socket '{}': {source}", path.display())]
    Bind {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to set socket permissions '{}': {source}", path.display())]
    Chmod {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("server accept error: {0}")]
    Accept(#[source] std::io::Error),
}

/// Map of active attach resize channels: service name → sender.
type ResizeMap = std::collections::HashMap<String, mpsc::Sender<(u16, u16)>>;

/// Shared state passed to all handlers.
#[derive(Clone)]
pub(crate) struct ApiState {
    pub cmd_tx: mpsc::UnboundedSender<RunnerCommand>,
    /// Runner event broadcast, used by `GET /events` to stream state changes.
    /// Subscribing per-request keeps the API decoupled from the runner loop —
    /// a slow HTTP client lags its own receiver and nobody else's.
    pub event_tx: broadcast::Sender<RunnerEvent>,
    /// Whether the runner's initial startup sweep has decided every item.
    /// Handlers wait on this from their own task, which is safe precisely
    /// because they are not the runner's command loop.
    pub startup_settled: watch::Receiver<bool>,
    /// Resize channels for active attach sessions. The attach bridge task
    /// registers its receiver here; the resize HTTP handler sends through it.
    pub attach_resize_txs: std::sync::Arc<tokio::sync::Mutex<ResizeMap>>,
}

/// Bind the unix socket at `socket_path` and chmod it to 0o600 so only the
/// owner can connect. Removes any stale socket file first. Returns the
/// listener on success; errors surface synchronously so the runner can log
/// them visibly at startup.
pub fn bind_api(socket_path: &Path) -> Result<UnixListener, ServerError> {
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| ServerError::Bind {
            path: socket_path.to_path_buf(),
            source,
        })?;
    }
    // Remove stale socket file (crashed previous run, etc.).
    let _ = std::fs::remove_file(socket_path);

    let listener = UnixListener::bind(socket_path).map_err(|source| ServerError::Bind {
        path: socket_path.to_path_buf(),
        source,
    })?;

    // Restrict to owner-only. The API can stop services / read logs; anyone
    // else on the box shouldn't get to drive it.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600)).map_err(
            |source| ServerError::Chmod {
                path: socket_path.to_path_buf(),
                source,
            },
        )?;
    }

    Ok(listener)
}

/// Serve the API on a pre-bound listener until `shutdown` is signalled.
///
/// The socket file at `socket_path` is removed on exit (including panic,
/// via the [`SocketGuard`] Drop impl).
pub async fn serve_api(
    listener: UnixListener,
    socket_path: PathBuf,
    cmd_tx: mpsc::UnboundedSender<RunnerCommand>,
    event_tx: broadcast::Sender<RunnerEvent>,
    startup_settled: watch::Receiver<bool>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), ServerError> {
    let _guard = SocketGuard(socket_path);
    let state = Arc::new(ApiState {
        cmd_tx,
        event_tx,
        startup_settled,
        attach_resize_txs: std::sync::Arc::new(tokio::sync::Mutex::new(
            std::collections::HashMap::new(),
        )),
    });
    let app = routes::build_router(state);
    accept_loop(listener, app, shutdown).await
}

/// Serve an arbitrary router on a pre-bound unix listener until `shutdown`.
///
/// Same lifecycle as [`serve_api`] — including removing the socket file on
/// exit — but for callers that build their own router. The daemon's control
/// plane ([`crate::daemon`]) uses this.
pub(crate) async fn serve_router(
    listener: UnixListener,
    socket_path: PathBuf,
    app: axum::Router,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), ServerError> {
    let _guard = SocketGuard(socket_path);
    accept_loop(listener, app, shutdown).await
}

/// RAII guard that removes the socket file on drop (normal exit or panic).
struct SocketGuard(PathBuf);

impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Serve `app` on `listener` until `shutdown` flips to true.
///
/// Shared by the project API and the daemon's control socket
/// ([`crate::daemon`]) — both are axum routers over a `UnixListener`, so
/// neither needs its own accept loop.
pub(crate) async fn accept_loop(
    listener: UnixListener,
    app: axum::Router,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), ServerError> {
    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (stream, _addr) = accept.map_err(ServerError::Accept)?;
                let io = TokioIo::new(stream);
                let tower_service = app.clone();
                tokio::spawn(async move {
                    let hyper_service =
                        hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                            use tower::ServiceExt;
                            tower_service.clone().oneshot(req)
                        });
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection_with_upgrades(io, hyper_service)
                        .await;
                });
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return Ok(());
                }
            }
        }
    }
}
