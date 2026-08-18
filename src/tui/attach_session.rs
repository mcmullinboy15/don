//! The tasks behind an attach window: raw stdin in, process output out.
//!
//! Two directions, two tasks, and one rule that shapes both: while the window
//! is open the crossterm event stream is *suspended* and stdin is read raw.
//!
//! Parsing stdin into key events and re-encoding them into bytes for the
//! process would lose exactly the things an interactive program cares about —
//! the precise escape sequence a terminal sent, modifier encodings, bracketed
//! paste. Reading raw and forwarding bytes untouched is both more faithful and
//! less code; the only bytes don keeps for itself are the ones behind the
//! `Ctrl+P` prefix (see [`super::attach_window`]).

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use super::attach_window::{AttachInput, KeyRouter};
use super::events::{AppEvent, AttachEvent};
use crate::output::emulator::EmulatorHandle;

/// Everything the main loop needs to keep talking to a live session.
pub(crate) struct Session {
    pub(crate) name: String,
    /// The client's own emulator, holding this process's screen.
    pub(crate) emulator: EmulatorHandle,
    /// Identifies the session to the resize endpoint.
    pub(crate) session_id: Option<u64>,
    stdin_task: tokio::task::JoinHandle<()>,
    output_task: tokio::task::JoinHandle<()>,
    writer_task: tokio::task::JoinHandle<()>,
}

impl Session {
    /// Stop every task. The process keeps running — this closes don's view of
    /// it, which is what detaching means.
    pub(crate) fn shutdown(self) {
        self.stdin_task.abort();
        self.output_task.abort();
        self.writer_task.abort();
    }
}

/// Open a session and start pumping it.
///
/// `cols`/`rows` are the grid the process should believe it has — the
/// window's inner rectangle, so what it draws fits the box it is drawn in.
pub(crate) async fn start(
    socket_path: &std::path::Path,
    name: &str,
    cols: u16,
    rows: u16,
    events: mpsc::Sender<AppEvent>,
) -> Result<Session, crate::client::ClientError> {
    let session = crate::client::attach_session::open(socket_path, name, cols, rows).await?;

    // The client's own emulator: this TUI may be remote, so the screen has to
    // be reconstructed here from the byte stream the server sends. The server
    // opens that stream with a coherent repaint of the current grid, so the
    // screen is right from the first frame rather than filling in as the
    // process happens to redraw.
    let emulator = crate::output::emulator::spawn_emulator_thread();
    emulator.register(name, cols, rows);
    if !session.leftover.is_empty() {
        emulator.feed(name, session.leftover.clone());
    }

    let (to_process, mut outbound) = mpsc::unbounded_channel::<Vec<u8>>();

    // Process input. A task of its own so a send from the main loop never
    // waits on the socket.
    let mut writer = session.writer;
    let writer_task = tokio::spawn(async move {
        while let Some(bytes) = outbound.recv().await {
            if writer.write_all(&bytes).await.is_err() {
                break;
            }
            if writer.flush().await.is_err() {
                break;
            }
        }
    });

    // Process output → this client's emulator.
    let output_task = tokio::spawn({
        let emulator = emulator.clone();
        let name = name.to_string();
        let events = events.clone();
        let mut reader = session.reader;
        async move {
            let mut buf = [0u8; 8192];
            loop {
                match reader.read(&mut buf).await {
                    Ok(0) | Err(_) => {
                        let _ = events
                            .send(AppEvent::Attach(AttachEvent::Ended(Some(format!(
                                "'{name}' detached (process exited or restarted)"
                            )))))
                            .await;
                        return;
                    }
                    Ok(n) => {
                        emulator.feed(&name, buf[..n].to_vec());
                        if events
                            .send(AppEvent::Attach(AttachEvent::Output))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
            }
        }
    });

    // Raw stdin → the router → either the process or the main loop.
    let stdin_task = tokio::spawn({
        let events = events.clone();
        async move {
            let mut stdin = tokio::io::stdin();
            let mut router = KeyRouter::default();
            let mut buf = [0u8; 4096];
            loop {
                let n = match stdin.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                for outcome in router.route(&buf[..n]) {
                    match outcome {
                        AttachInput::Forward(bytes) => {
                            if to_process.send(bytes).is_err() {
                                return;
                            }
                        }
                        AttachInput::Pending => {}
                        command => {
                            if events
                                .send(AppEvent::Attach(AttachEvent::Command(command)))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                }
            }
        }
    });

    Ok(Session {
        name: name.to_string(),
        emulator,
        session_id: session.session_id,
        stdin_task,
        output_task,
        writer_task,
    })
}

/// Tell the server the window's grid changed size. Fire-and-forget: a failed
/// resize leaves the process with a stale idea of its grid, which is a
/// cosmetic problem, not a reason to tear the session down.
pub(crate) fn notify_resize(
    socket_path: Arc<std::path::PathBuf>,
    name: String,
    session_id: Option<u64>,
    cols: u16,
    rows: u16,
    emulator: EmulatorHandle,
) {
    emulator.resize(&name, cols, rows);
    tokio::spawn(async move {
        let _ = crate::client::attach_session::resize(&socket_path, &name, session_id, cols, rows)
            .await;
    });
}
