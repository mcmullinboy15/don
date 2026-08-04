//! HTTP API behind the web UI.
//!
//! Every handler does the same three things: resolve a project id to an entry
//! via [`ProjectDirectory`], open a [`Client`] on that project's existing
//! unix socket, and forward. There is no second implementation of anything —
//! whatever `don status` or `don restart` can do, the UI does through the
//! same endpoints, so the two can't drift.
//!
//! Streams (logs, state changes) are re-published as Server-Sent Events,
//! which is what browsers can consume natively. The project side of the wire
//! stays NDJSON.

use super::directory::ProjectDirectory;
use crate::client::{Client, ClientError, RunTaskOptions};
use crate::daemon::registry::ProjectEntry;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;

/// Buffer for a single browser's stream. Deep enough to ride out a render
/// stall, shallow enough that a tab left open on a chatty service doesn't
/// grow without bound — an over-full channel drops the connection, and the
/// browser's automatic SSE reconnect picks things back up.
const STREAM_BUFFER: usize = 256;

/// State every API handler needs.
#[derive(Clone)]
pub(crate) struct ApiState {
    pub directory: ProjectDirectory,
}

/// Build the `/api` router.
pub(crate) fn build_router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/projects", get(list_projects))
        .route("/projects/{id}", get(get_project))
        .route("/projects/{id}/status", get(get_status))
        .route("/projects/{id}/ports", get(get_ports))
        .route("/projects/{id}/events", get(stream_events))
        .route("/projects/{id}/logs/{name}", get(get_logs))
        .route("/projects/{id}/logs/{name}/stream", get(stream_logs))
        .route("/projects/{id}/start/{name}", post(post_start))
        .route("/projects/{id}/stop/{name}", post(post_stop))
        .route("/projects/{id}/restart/{name}", post(post_restart))
        .route("/projects/{id}/run/{name}", post(post_run))
        .route("/projects/{id}/run-pending", post(post_run_pending))
        .route(
            "/projects/{id}/completions/{task}/{param}",
            post(post_completions),
        )
        .route("/projects/{id}/shutdown", post(post_shutdown))
        .with_state(state)
}

#[derive(Serialize)]
struct ProjectsResponse {
    projects: Vec<ProjectEntry>,
}

/// `GET /api/projects` — every project the UI can show.
async fn list_projects(State(state): State<Arc<ApiState>>) -> Response {
    let projects = state.directory.list().await;
    Json(ProjectsResponse { projects }).into_response()
}

/// `GET /api/projects/:id` — one project's registration metadata.
async fn get_project(State(state): State<Arc<ApiState>>, Path(id): Path<String>) -> Response {
    match state.directory.get(&id).await {
        Some(entry) => Json(entry).into_response(),
        None => unknown_project(&id),
    }
}

#[derive(Deserialize)]
struct StatusQuery {
    #[serde(default)]
    verbose: bool,
    #[serde(default)]
    name: Option<String>,
}

/// `GET /api/projects/:id/status` — the same payload as `don status`.
async fn get_status(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    Query(query): Query<StatusQuery>,
) -> Response {
    let Some(client) = resolve(&state, &id).await else {
        return unknown_project(&id);
    };
    match client.status(query.verbose, query.name.as_deref()).await {
        Ok(items) => Json(serde_json::json!({ "items": items })).into_response(),
        Err(e) => map_client_error(e),
    }
}

/// `GET /api/projects/:id/ports` — the runtime port manifest.
///
/// Read straight off disk rather than through the project API: `don ports`
/// does the same, and it means the UI can still show bound addresses for a
/// project whose runner is mid-restart.
async fn get_ports(State(state): State<Arc<ApiState>>, Path(id): Path<String>) -> Response {
    let Some(entry) = state.directory.get(&id).await else {
        return unknown_project(&id);
    };
    match crate::ports::read_manifest(&entry.root) {
        Ok(manifest) => Json(manifest).into_response(),
        // No manifest means nothing needed one — an empty result, not a fault.
        Err(_) => Json(crate::ports::PortManifest::default()).into_response(),
    }
}

/// `GET /api/projects/:id/events` — state changes as Server-Sent Events.
async fn stream_events(State(state): State<Arc<ApiState>>, Path(id): Path<String>) -> Response {
    let Some(client) = resolve(&state, &id).await else {
        return unknown_project(&id);
    };
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(STREAM_BUFFER);
    tokio::spawn(async move {
        let _ = client.events_follow(|line| forward(&tx, line)).await;
    });
    sse(rx)
}

#[derive(Deserialize)]
struct LogsQuery {
    #[serde(default = "default_last")]
    last: usize,
}

fn default_last() -> usize {
    200
}

/// `GET /api/projects/:id/logs/:name` — a snapshot of the ring buffer.
async fn get_logs(
    State(state): State<Arc<ApiState>>,
    Path((id, name)): Path<(String, String)>,
    Query(query): Query<LogsQuery>,
) -> Response {
    let Some(client) = resolve(&state, &id).await else {
        return unknown_project(&id);
    };
    match client.logs(&name, query.last).await {
        Ok(lines) => Json(serde_json::json!({ "name": name, "lines": lines })).into_response(),
        Err(e) => map_client_error(e),
    }
}

/// `GET /api/projects/:id/logs/:name/stream` — a live tail as SSE.
async fn stream_logs(
    State(state): State<Arc<ApiState>>,
    Path((id, name)): Path<(String, String)>,
    Query(query): Query<LogsQuery>,
) -> Response {
    let Some(client) = resolve(&state, &id).await else {
        return unknown_project(&id);
    };
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(STREAM_BUFFER);
    tokio::spawn(async move {
        let _ = client
            .logs_follow(&name, query.last, |line| forward(&tx, line))
            .await;
    });
    sse(rx)
}

/// `POST /api/projects/:id/start/:name`
async fn post_start(
    State(state): State<Arc<ApiState>>,
    Path((id, name)): Path<(String, String)>,
) -> Response {
    control(
        &state,
        &id,
        |client| async move { client.start(&name).await },
    )
    .await
}

/// `POST /api/projects/:id/stop/:name`
async fn post_stop(
    State(state): State<Arc<ApiState>>,
    Path((id, name)): Path<(String, String)>,
) -> Response {
    control(
        &state,
        &id,
        |client| async move { client.stop(&name).await },
    )
    .await
}

/// `POST /api/projects/:id/restart/:name`
async fn post_restart(
    State(state): State<Arc<ApiState>>,
    Path((id, name)): Path<(String, String)>,
) -> Response {
    control(
        &state,
        &id,
        |client| async move { client.restart(&name).await },
    )
    .await
}

/// `POST /api/projects/:id/shutdown` — stop the project's whole stack.
async fn post_shutdown(State(state): State<Arc<ApiState>>, Path(id): Path<String>) -> Response {
    control(&state, &id, |client| async move { client.shutdown().await }).await
}

#[derive(Default, Deserialize)]
struct RunBody {
    #[serde(default)]
    params: HashMap<String, String>,
    #[serde(default)]
    wait: bool,
    #[serde(default)]
    wait_timeout: Option<String>,
}

/// `POST /api/projects/:id/run/:name` — run a task, optionally with params.
async fn post_run(
    State(state): State<Arc<ApiState>>,
    Path((id, name)): Path<(String, String)>,
    body: Option<Json<RunBody>>,
) -> Response {
    let body = body.map(|Json(b)| b).unwrap_or_default();
    control(&state, &id, |client| async move {
        client
            .run_task_with_options(
                &name,
                body.params,
                RunTaskOptions {
                    wait: body.wait,
                    wait_timeout: body.wait_timeout,
                },
            )
            .await
    })
    .await
}

/// `POST /api/projects/:id/run-pending` — run every task awaiting a trigger.
async fn post_run_pending(State(state): State<Arc<ApiState>>, Path(id): Path<String>) -> Response {
    control(
        &state,
        &id,
        |client| async move { client.run_pending().await },
    )
    .await
}

#[derive(Default, Deserialize)]
struct CompletionsBody {
    #[serde(default)]
    partial: HashMap<String, String>,
    #[serde(default)]
    force_refresh: bool,
}

/// `POST /api/projects/:id/completions/:task/:param` — candidate values for
/// one task param, so the run dialog can offer the same choices the CLI does.
async fn post_completions(
    State(state): State<Arc<ApiState>>,
    Path((id, task, param)): Path<(String, String, String)>,
    body: Option<Json<CompletionsBody>>,
) -> Response {
    let Some(client) = resolve(&state, &id).await else {
        return unknown_project(&id);
    };
    let body = body.map(|Json(b)| b).unwrap_or_default();
    match client
        .resolve_completions(&task, &param, body.partial, body.force_refresh)
        .await
    {
        Ok(values) => Json(serde_json::json!({ "values": values })).into_response(),
        Err(e) => map_client_error(e),
    }
}

// --- helpers ---

/// Resolve an id to a client for that project's API socket.
async fn resolve(state: &ApiState, id: &str) -> Option<Client> {
    let entry = state.directory.get(id).await?;
    Some(Client::with_socket_path(entry.socket))
}

/// Run a control call that returns `()` and map the result to a response.
async fn control<F, Fut>(state: &ApiState, id: &str, call: F) -> Response
where
    F: FnOnce(Client) -> Fut,
    Fut: std::future::Future<Output = Result<(), ClientError>>,
{
    let Some(client) = resolve(state, id).await else {
        return unknown_project(id);
    };
    match call(client).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => map_client_error(e),
    }
}

/// Push one NDJSON line into a browser stream as an SSE message.
///
/// Returning `Err` stops the follow loop, which is how a closed tab
/// propagates back and releases the project-side subscription.
fn forward(
    tx: &tokio::sync::mpsc::Sender<Result<Event, Infallible>>,
    line: &str,
) -> Result<(), ClientError> {
    tx.try_send(Ok(Event::default().data(line)))
        .map_err(|_| ClientError::Invalid("browser stream closed".into()))
}

fn sse(rx: tokio::sync::mpsc::Receiver<Result<Event, Infallible>>) -> Response {
    Sse::new(ReceiverStream::new(rx))
        .keep_alive(KeepAlive::default())
        .into_response()
}

/// Translate a project-side failure into the same status the project API
/// used, so the UI sees one consistent error vocabulary end to end.
fn map_client_error(error: ClientError) -> Response {
    let status = match &error {
        ClientError::NotFound { .. } => StatusCode::NOT_FOUND,
        ClientError::BadRequest { .. } => StatusCode::BAD_REQUEST,
        ClientError::Conflict { .. } => StatusCode::CONFLICT,
        ClientError::WaitTimeout { .. } => StatusCode::REQUEST_TIMEOUT,
        ClientError::CommandFailed { .. } | ClientError::Completion(_) => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        // The project was registered a moment ago but its socket has gone —
        // it shut down between the registry read and this call.
        ClientError::NotRunning { .. } => StatusCode::SERVICE_UNAVAILABLE,
        ClientError::Server { .. }
        | ClientError::Io(_)
        | ClientError::Invalid(_)
        | ClientError::Json(_) => StatusCode::BAD_GATEWAY,
    };
    let mut body = serde_json::json!({ "error": error.to_string() });
    if let ClientError::Completion(completion) = &error
        && let Some(log_path) = &completion.log_path
    {
        body["log_path"] = serde_json::json!(log_path);
    }
    (status, Json(body)).into_response()
}

fn unknown_project(id: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({
            "error": format!("no running project with id '{id}' — it may have shut down"),
        })),
    )
        .into_response()
}
