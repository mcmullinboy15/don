//! Control API for the daemon, served over its unix socket.
//!
//! This is the *private* side of the daemon — how `don start` announces
//! itself and how `don daemon status|stop` reaches in. The browser never
//! talks to it; the web UI has its own router in [`crate::web`].
//!
//! Like the project API, handlers own no state. They send a
//! [`DaemonCommand`] to the task that owns the registry and await a
//! `oneshot` reply, which keeps the registry single-owner and free of locks.

use super::registry::ProjectEntry;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// A request into the task that owns the project registry.
#[derive(Debug)]
pub(crate) enum DaemonCommand {
    /// Add or replace a project registration.
    Register {
        entry: Box<ProjectEntry>,
        reply: oneshot::Sender<()>,
    },
    /// Withdraw a project. Replies with whether anything was removed.
    Deregister {
        id: String,
        reply: oneshot::Sender<bool>,
    },
    /// List live projects, pruning unreachable ones first.
    List {
        reply: oneshot::Sender<Vec<ProjectEntry>>,
    },
    /// Count live projects, for `GET /info`.
    Count { reply: oneshot::Sender<usize> },
    /// Stop the daemon. Registered projects keep running.
    Shutdown,
}

/// State shared by the control handlers.
#[derive(Clone)]
pub(crate) struct ControlState {
    pub cmd_tx: mpsc::UnboundedSender<DaemonCommand>,
    /// Address the web UI is bound to, reported by `GET /info`. `None` when
    /// the daemon is running its control plane without a web server.
    pub web_addr: Option<String>,
}

/// Build the control router.
pub(crate) fn build_router(state: Arc<ControlState>) -> Router {
    Router::new()
        .route("/info", get(get_info))
        .route("/projects", get(get_projects).post(post_project))
        .route("/projects/{id}", delete(delete_project))
        .route("/shutdown", post(post_shutdown))
        .with_state(state)
}

#[derive(Serialize)]
struct InfoResponse {
    version: &'static str,
    pid: u32,
    web_addr: Option<String>,
    projects: usize,
}

/// `GET /info` — what daemon is this, and where is the UI?
async fn get_info(State(state): State<Arc<ControlState>>) -> Response {
    let Some(projects) = dispatch(&state, |reply| DaemonCommand::Count { reply }).await else {
        return unavailable();
    };
    Json(InfoResponse {
        version: env!("CARGO_PKG_VERSION"),
        pid: std::process::id(),
        web_addr: state.web_addr.clone(),
        projects,
    })
    .into_response()
}

#[derive(Serialize)]
struct ProjectsResponse {
    projects: Vec<ProjectEntry>,
}

/// `GET /projects` — every project that still answers on its socket.
async fn get_projects(State(state): State<Arc<ControlState>>) -> Response {
    match dispatch(&state, |reply| DaemonCommand::List { reply }).await {
        Some(projects) => Json(ProjectsResponse { projects }).into_response(),
        None => unavailable(),
    }
}

/// `POST /projects` — register a running project.
///
/// Idempotent: re-registering the same project root replaces the previous
/// entry, which is what a restarted `don start` needs.
async fn post_project(
    State(state): State<Arc<ControlState>>,
    Json(entry): Json<ProjectEntry>,
) -> Response {
    match dispatch(&state, move |reply| DaemonCommand::Register {
        entry: Box::new(entry),
        reply,
    })
    .await
    {
        Some(()) => StatusCode::NO_CONTENT.into_response(),
        None => unavailable(),
    }
}

/// `DELETE /projects/:id` — withdraw a project.
///
/// Deleting an unknown id succeeds. The caller is `don start` on its way out,
/// and "it wasn't registered" and "it is no longer registered" are the same
/// outcome from its point of view.
async fn delete_project(
    State(state): State<Arc<ControlState>>,
    Path(id): Path<String>,
) -> Response {
    match dispatch(&state, move |reply| DaemonCommand::Deregister { id, reply }).await {
        Some(_) => StatusCode::NO_CONTENT.into_response(),
        None => unavailable(),
    }
}

/// `POST /shutdown` — stop the daemon, leaving every project running.
async fn post_shutdown(State(state): State<Arc<ControlState>>) -> Response {
    if state.cmd_tx.send(DaemonCommand::Shutdown).is_err() {
        return unavailable();
    }
    StatusCode::NO_CONTENT.into_response()
}

/// Send a command carrying a `oneshot` reply and await it. `None` means the
/// registry task is gone — the daemon is shutting down.
async fn dispatch<T, F>(state: &ControlState, build: F) -> Option<T>
where
    F: FnOnce(oneshot::Sender<T>) -> DaemonCommand,
{
    let (tx, rx) = oneshot::channel();
    state.cmd_tx.send(build(tx)).ok()?;
    rx.await.ok()
}

fn unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({ "error": "don daemon is shutting down" })),
    )
        .into_response()
}
