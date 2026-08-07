//! Raw stream handler for interactive attach sessions.
//!
//! After an HTTP upgrade handshake, the connection becomes a raw bidirectional
//! byte stream — stdin bytes flow in, PTY output bytes flow out, with zero
//! framing overhead. Resize events arrive via a separate POST endpoint.

use super::{ApiState, NameSessions};
use axum::extract::{Json, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

/// Query parameters for the attach upgrade request.
#[derive(Deserialize)]
pub(crate) struct AttachParams {
    pid: u32,
    cols: u16,
    rows: u16,
}

/// Body for the resize endpoint. `session` is the id issued in the attach
/// response's `x-don-attach-session` header; without it (an older client),
/// the resize applies to every session the client could mean.
#[derive(Deserialize)]
pub(crate) struct ResizeBody {
    cols: u16,
    rows: u16,
    #[serde(default)]
    session: Option<u64>,
}

/// The effective size for an process: the smallest attached client wins each
/// dimension, so every client sees the whole grid (tmux-style letterboxing).
fn effective_size(sizes: &std::collections::HashMap<u64, (u16, u16)>) -> Option<(u16, u16)> {
    let cols = sizes.values().map(|(c, _)| *c).min()?;
    let rows = sizes.values().map(|(_, r)| *r).min()?;
    Some((cols, rows))
}

/// Recompute and apply the effective grid size for `name`. Retains the last
/// size when no sessions remain.
async fn apply_effective_size(state: &ApiState, name: &str) {
    let (gate, size) = {
        let map = state.attach_sessions.lock().await;
        let Some(sessions) = map.get(name) else {
            return;
        };
        match effective_size(&sessions.sizes) {
            Some(size) => (sessions.gate.clone(), size),
            None => return,
        }
    };
    let _ = gate
        .send(crate::output::PtyInput::Resize(size.0, size.1))
        .await;
    state.emulator.resize(name, size.0, size.1);
}

/// `GET /attach/{name}?pid=N&cols=C&rows=R` — upgrade to raw stream.
///
/// Validates the `Upgrade: don-attach` header, requests an attach session
/// from the runner (blocking until the service/task is ready), then responds
/// with `101 Switching Protocols`. After the upgrade, the connection carries
/// raw stdin/stdout bytes with zero framing.
pub(crate) async fn attach_handler(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
    Query(params): Query<AttachParams>,
    request: axum::http::Request<axum::body::Body>,
) -> Response {
    // Validate upgrade header.
    let upgrade_ok = request
        .headers()
        .get("upgrade")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("don-attach"));
    if !upgrade_ok {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({"error": "expected Upgrade: don-attach"})),
        )
            .into_response();
    }

    // Attach straight through the process's output state — the supervisor
    // registered the live spawn's gate there; no runner round trip.
    let session = match state.attach.attach(&name, params.pid).await {
        Ok(session) => session,
        Err(e) => {
            let status = match e {
                crate::runner::CommandError::UnknownService { .. } => StatusCode::NOT_FOUND,
                crate::runner::CommandError::InvalidState { .. } => StatusCode::CONFLICT,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            return (
                status,
                axum::Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    let pty_input = session.pty_input;
    let output_rx = session.output_rx;
    // Detach-on-drop: however the bridge task ends, releasing this guard is
    // what decrements the client count and resumes prefixed stdout.
    let attach_guard = session.guard;

    // Register this session and apply the new effective size — the smallest
    // attached client wins each dimension.
    let session_id = {
        let mut map = state.attach_sessions.lock().await;
        let sessions = map.entry(name.clone()).or_insert_with(|| NameSessions {
            next_id: 0,
            gate: pty_input.clone(),
            sizes: std::collections::HashMap::new(),
        });
        // A restart hands out a fresh gate; keep the stored one current.
        sessions.gate = pty_input.clone();
        let id = sessions.next_id;
        sessions.next_id += 1;
        sessions.sizes.insert(id, (params.cols, params.rows));
        id
    };
    apply_effective_size(&state, &name).await;

    // Spawn background task to handle the upgraded connection.
    let state_clone = state.clone();
    let name_clone = name.clone();
    tokio::spawn(async move {
        let _attach_guard = attach_guard;
        let upgraded = match hyper::upgrade::on(request).await {
            Ok(upgraded) => upgraded,
            Err(_) => {
                end_session(&state_clone, &name_clone, session_id).await;
                return;
            }
        };

        let io = hyper_util::rt::TokioIo::new(upgraded);
        bridge_raw(io, pty_input, output_rx).await;

        end_session(&state_clone, &name_clone, session_id).await;
    });

    // Return 101 Switching Protocols, with the session id the client echoes
    // in resize requests.
    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header("connection", "Upgrade")
        .header("upgrade", "don-attach")
        .header("x-don-attach-session", session_id.to_string())
        .body(axum::body::Body::empty())
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// Remove a session and re-apply the effective size for the remaining
/// clients (none remaining retains the last size — no SIGWINCH churn for
/// the running program).
async fn end_session(state: &ApiState, name: &str, session_id: u64) {
    let remaining = {
        let mut map = state.attach_sessions.lock().await;
        let Some(sessions) = map.get_mut(name) else {
            return;
        };
        sessions.sizes.remove(&session_id);
        let remaining = !sessions.sizes.is_empty();
        if !remaining {
            map.remove(name);
        }
        remaining
    };
    if remaining {
        apply_effective_size(state, name).await;
    }
}

/// `POST /attach/{name}/resize` — resize the attached PTY.
pub(crate) async fn resize_handler(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
    Json(body): Json<ResizeBody>,
) -> Response {
    let known = {
        let mut map = state.attach_sessions.lock().await;
        match map.get_mut(&name) {
            Some(sessions) => {
                match body.session {
                    Some(id) if sessions.sizes.contains_key(&id) => {
                        sessions.sizes.insert(id, (body.cols, body.rows));
                    }
                    Some(_) => {}
                    // Older client without a session id: the only honest
                    // reading is "this client is now this size" for every
                    // session it could mean.
                    None => {
                        for size in sessions.sizes.values_mut() {
                            *size = (body.cols, body.rows);
                        }
                    }
                }
                true
            }
            None => false,
        }
    };
    if !known {
        return (
            StatusCode::NOT_FOUND,
            axum::Json(
                serde_json::json!({"error": format!("no active attach session for '{name}'")}),
            ),
        )
            .into_response();
    }
    apply_effective_size(&state, &name).await;
    StatusCode::NO_CONTENT.into_response()
}

/// Bridge a raw bidirectional stream to the PTY's input gate. Each client
/// read becomes one atomic input frame; the gate interleaves frames from
/// every writer without shearing.
async fn bridge_raw<S: AsyncReadExt + AsyncWriteExt + Unpin>(
    io: S,
    pty_input: mpsc::Sender<crate::output::PtyInput>,
    mut output_rx: mpsc::Receiver<crate::output::SinkLine>,
) {
    use crate::output::PtyInput;

    let (mut io_read, mut io_write) = tokio::io::split(io);

    // Channel for OSC responses detected in the output stream.
    let (osc_tx, mut osc_rx) = mpsc::channel::<bytes::Bytes>(8);

    // Task: output_rx → client (raw bytes) + OSC detection.
    let output_to_client = async move {
        while let Some(sink_line) = output_rx.recv().await {
            for response in crate::output::osc::find_responses(&sink_line.line) {
                let _ = osc_tx.try_send(bytes::Bytes::from_static(response));
            }
            if io_write.write_all(&sink_line.line).await.is_err() {
                break;
            }
        }
    };

    // Task: client → gate (input frames) + OSC responses + resize.
    let client_to_pty = async move {
        let mut buf = [0u8; 4096];
        loop {
            tokio::select! {
                result = io_read.read(&mut buf) => {
                    match result {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if pty_input.send(PtyInput::Frame(buf[..n].to_vec())).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                Some(data) = osc_rx.recv() => {
                    let _ = pty_input.send(PtyInput::Frame(data.to_vec())).await;
                }
            }
        }
    };

    tokio::select! {
        _ = output_to_client => (),
        _ = client_to_pty => (),
    }
}
