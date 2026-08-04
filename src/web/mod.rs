//! The web UI server.
//!
//! One router, two hosts. The daemon serves it over every registered project;
//! `don start --with-ui` serves the identical thing over the single project
//! it is running. The difference is confined to which [`ProjectDirectory`]
//! the router is built with — no handler knows or cares which mode it's in.
//!
//! The server binds loopback only and does not authenticate: anything that
//! can reach the port is already running on this machine, and so can already
//! do everything don can. See [`origin`] for the one cross-origin case that
//! *is* checked, and why it's different.

mod api;
mod assets;
mod directory;
mod origin;

pub(crate) use directory::ProjectDirectory;

use axum::Router;
use axum::routing::get;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

/// The default port `don`'s web UI listens on.
///
/// 3666 spells "don" on a phone keypad, and sits outside the 3000/5173/8080
/// range dev servers fight over.
pub const DEFAULT_PORT: u16 = 3666;

/// Errors starting the web server.
#[derive(Debug, thiserror::Error)]
pub enum WebError {
    #[error("failed to bind the web ui to {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("web ui server error: {0}")]
    Serve(#[source] std::io::Error),
}

/// The default address: loopback on [`DEFAULT_PORT`].
pub fn default_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], DEFAULT_PORT))
}

/// Bind the web UI, returning the listener and the address actually bound.
///
/// Binding happens separately from serving so callers can report a real
/// address (port 0 resolves to a real port here) and so a port conflict
/// surfaces immediately instead of inside a background task.
pub async fn bind(addr: SocketAddr) -> Result<(TcpListener, SocketAddr), WebError> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|source| WebError::Bind { addr, source })?;
    let bound = listener.local_addr().map_err(WebError::Serve)?;
    Ok((listener, bound))
}

/// Serve the UI on a pre-bound listener until `shutdown` flips to true.
pub(crate) async fn serve(
    listener: TcpListener,
    directory: ProjectDirectory,
    port: u16,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), WebError> {
    let router = build_router(directory, port);
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            while shutdown.changed().await.is_ok() {
                if *shutdown.borrow() {
                    return;
                }
            }
        })
        .await
        .map_err(WebError::Serve)
}

/// Serve the UI over a single project, for `don start --with-ui`.
///
/// The public entry point for the project-local mode: `ProjectDirectory` is
/// internal, so callers outside the crate hand over a [`ProjectEntry`] and
/// get the same router the daemon serves.
pub async fn serve_single(
    listener: TcpListener,
    project: crate::daemon::ProjectEntry,
    port: u16,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), WebError> {
    serve(
        listener,
        ProjectDirectory::Single(Box::new(project)),
        port,
        shutdown,
    )
    .await
}

/// Assemble the router: `/api` plus the embedded bundle, both behind the
/// origin guard.
fn build_router(directory: ProjectDirectory, port: u16) -> Router {
    let origin_state = Arc::new(origin::OriginState { port });
    let api_state = Arc::new(api::ApiState { directory });

    Router::new()
        .nest("/api", api::build_router(api_state))
        .fallback(get(assets::serve))
        .layer(axum::middleware::from_fn_with_state(
            origin_state,
            origin::guard,
        ))
}
