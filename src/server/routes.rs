//! HTTP endpoints for the unix socket API.

use super::ApiState;
use crate::runner::{CommandError, ItemStatus, RunnerCommand};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::oneshot;

/// Build the axum router for the API.
pub(crate) fn build_router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/status", get(get_status))
        .route("/start/{name}", post(post_start))
        .route("/stop/{name}", post(post_stop))
        .route("/restart/{name}", post(post_restart))
        .route("/logs/{name}", get(get_logs))
        .route("/attach/{name}", get(super::attach::attach_handler))
        .route("/attach/{name}/resize", post(super::attach::resize_handler))
        .route("/run-pending", post(post_run_pending))
        .with_state(state)
}

/// Query params for the status endpoint.
#[derive(serde::Deserialize)]
struct StatusQuery {
    #[serde(default)]
    verbose: bool,
}

/// `GET /status` — list all services/tasks and their current state.
async fn get_status(
    State(state): State<Arc<ApiState>>,
    axum::extract::Query(query): axum::extract::Query<StatusQuery>,
) -> Response {
    let (tx, rx) = oneshot::channel();
    if state
        .cmd_tx
        .send(RunnerCommand::Status {
            verbose: query.verbose,
            reply: tx,
        })
        .await
        .is_err()
    {
        return runner_unavailable();
    }
    match rx.await {
        Ok(statuses) => Json(StatusResponse { items: statuses }).into_response(),
        Err(_) => runner_unavailable(),
    }
}

#[derive(Serialize)]
struct StatusResponse {
    items: Vec<ItemStatus>,
}

/// `POST /start/:name` — start a stopped service.
async fn post_start(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
) -> Response {
    dispatch_control_cmd(state, &name, |name, reply| RunnerCommand::Start {
        name,
        reply,
    })
    .await
}

/// `POST /stop/:name` — stop a running service.
async fn post_stop(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
) -> Response {
    dispatch_control_cmd(state, &name, |name, reply| RunnerCommand::Stop {
        name,
        reply,
    })
    .await
}

/// `POST /restart/:name` — restart a service.
async fn post_restart(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
) -> Response {
    dispatch_control_cmd(state, &name, |name, reply| RunnerCommand::Restart {
        name,
        reply,
    })
    .await
}

#[derive(Deserialize)]
struct LogsQuery {
    #[serde(default = "default_last")]
    last: usize,
    #[serde(default)]
    follow: bool,
}

fn default_last() -> usize {
    100
}

#[derive(Serialize)]
struct LogsResponse {
    name: String,
    lines: Vec<String>,
}

/// `GET /logs/:name?last=N[&follow=true]` — read from the ring buffer.
///
/// - Without `follow`: returns last N lines as JSON.
/// - With `follow=true`: streams newline-delimited JSON objects (NDJSON)
///   — one `{"line":"..."}` per log line. Closes when the client disconnects.
async fn get_logs(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
    Query(query): Query<LogsQuery>,
) -> Response {
    if query.follow {
        return follow_logs(state, name, query.last).await;
    }

    let (tx, rx) = oneshot::channel();
    if state
        .cmd_tx
        .send(RunnerCommand::Logs {
            name: name.clone(),
            last_n: query.last,
            reply: tx,
        })
        .await
        .is_err()
    {
        return runner_unavailable();
    }
    match rx.await {
        Ok(Some(raw)) => {
            let lines: Vec<String> = if raw.is_empty() {
                Vec::new()
            } else {
                raw.split('\n').map(String::from).collect()
            };
            Json(LogsResponse { name, lines }).into_response()
        }
        Ok(None) => not_found(&name),
        Err(_) => runner_unavailable(),
    }
}

async fn follow_logs(state: Arc<ApiState>, name: String, last: usize) -> Response {
    let (tx, rx) = oneshot::channel();
    if state
        .cmd_tx
        .send(RunnerCommand::LogsFollow {
            name: name.clone(),
            last_n: last,
            reply: tx,
        })
        .await
        .is_err()
    {
        return runner_unavailable();
    }
    let sink_rx = match rx.await {
        Ok(Some(r)) => r,
        Ok(None) => return not_found(&name),
        Err(_) => return runner_unavailable(),
    };

    // Build an NDJSON stream: one `{"line":"..."}\n` per SinkLine.
    use tokio_stream::{wrappers::ReceiverStream, StreamExt};
    let stream = ReceiverStream::new(sink_rx).map(|sink_line| {
        let line_str = String::from_utf8_lossy(&sink_line.line).into_owned();
        let json = serde_json::json!({ "line": line_str });
        let mut chunk = serde_json::to_vec(&json).unwrap_or_default();
        chunk.push(b'\n');
        Ok::<_, std::convert::Infallible>(bytes::Bytes::from(chunk))
    });

    match axum::response::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/x-ndjson")
        .body(axum::body::Body::from_stream(stream))
    {
        Ok(resp) => resp,
        Err(_) => runner_unavailable(),
    }
}

/// `POST /run-pending` — run all tasks in PendingRun state.
async fn post_run_pending(State(state): State<Arc<ApiState>>) -> Response {
    let (tx, rx) = oneshot::channel();
    if state
        .cmd_tx
        .send(RunnerCommand::RunPendingTasks { reply: tx })
        .await
        .is_err()
    {
        return runner_unavailable();
    }
    match rx.await {
        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(error_body(&e.to_string())),
        )
            .into_response(),
        Err(_) => runner_unavailable(),
    }
}

// --- helpers ---

async fn dispatch_control_cmd<F>(state: Arc<ApiState>, name: &str, build: F) -> Response
where
    F: FnOnce(String, oneshot::Sender<Result<(), CommandError>>) -> RunnerCommand,
{
    let (tx, rx) = oneshot::channel();
    if state
        .cmd_tx
        .send(build(name.to_string(), tx))
        .await
        .is_err()
    {
        return runner_unavailable();
    }
    match rx.await {
        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(CommandError::UnknownService { .. })) => not_found(name),
        Ok(Err(e @ CommandError::NotAService { .. })) => {
            (StatusCode::BAD_REQUEST, Json(error_body(&e.to_string()))).into_response()
        }
        Ok(Err(e @ CommandError::InvalidState { .. })) => {
            (StatusCode::CONFLICT, Json(error_body(&e.to_string()))).into_response()
        }
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(error_body(&e.to_string())),
        )
            .into_response(),
        Err(_) => runner_unavailable(),
    }
}

fn runner_unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(error_body("runner is shutting down")),
    )
        .into_response()
}

fn not_found(name: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(error_body(&format!("no service or task named '{name}'"))),
    )
        .into_response()
}

fn error_body(message: &str) -> serde_json::Value {
    serde_json::json!({ "error": message })
}
